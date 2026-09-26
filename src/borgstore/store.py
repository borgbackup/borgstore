"""
Key/value store implementation.

The Store uses a backend to store key/value data and adds some functionality:

- backend creation from a URL
- configurable nesting
- recursive list method
- soft deletion
- thread safety (one operation at a time, see Store docstring)
"""

from binascii import hexlify
from collections import Counter, OrderedDict
from contextlib import contextmanager
import enum
from functools import wraps
import logging
import os
import threading
import time
from typing import Generator, Iterator, NamedTuple, Optional

from .utils.nesting import nest, unnest
from .backends._base import ItemInfo, BackendBase, StoreValue, validate_value, validate_sources
from .backends.errors import ObjectNotFound, NoBackendGiven, BackendURLInvalid, ReadRangeError  # noqa
from .backends.posixfs import get_file_backend
from .backends.rclone import get_rclone_backend
from .backends.sftp import get_sftp_backend
from .backends.s3 import get_s3_backend
from .backends.rest import get_rest_backend
from .constants import DEL_SUFFIX, ROOTNS

logger = logging.getLogger(__name__)

# a cache namespace with a size limit is rescanned after this store has inserted more than
# size / CACHE_RESCAN_DIVISOR bytes into it, see Store._cache_scan.
CACHE_RESCAN_DIVISOR = 4
# such a rescan is done in steps, so it does not block the store for long if the cache has a lot of
# items: each item that is put into the cache continues the scan for about that long [s].
CACHE_SCAN_STEP_TIME = 0.005


class CacheMode(enum.Enum):
    C_OFF = "off"
    C_MIRROR = "mirror"
    C_WRITETHROUGH = "writethrough"

    @classmethod
    def from_str(cls, value):
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            try:
                return cls(value.lower())
            except ValueError as err:
                raise ValueError(f"unknown CacheMode: {value!r}") from err
        raise ValueError(f"unknown CacheMode: {value!r}")


class CachePolicy(NamedTuple):
    mode: CacheMode
    max_age: Optional[float]
    size: Optional[int]


class CacheScan:
    """State of a scan of a cache namespace that is in progress, see Store._cache_scan."""

    def __init__(self, infos: Generator[ItemInfo, None, None]):
        self.infos = infos  # lists the namespace (items and directories)
        self.seen: dict = {}  # nested name -> (size, last access timestamp), as listed
        self.changed: set = set()  # names the store has added, used or removed since the scan started
        self.steps = 0
        self.time = 0.0  # time spent scanning [s]


class CacheIndex:
    """
    In-memory view of one cache namespace that has a max_age or size limit.

    It tells the Store what to evict without listing the cache backend for each operation.
    It is this store's view only: what other clients sharing the same cache add, use or
    evict is only seen when the namespace is scanned, see Store._cache_scan.
    """

    def __init__(self, namespace: str, policy: CachePolicy):
        self.namespace = namespace
        self.policy = policy
        # nested name -> (size, last access timestamp), least recently used entry first.
        self.entries: OrderedDict = OrderedDict()
        self.total = 0  # sum of the entries' sizes
        self.inserted = 0  # bytes this store has put into the cache since it started the last scan
        self.scan: Optional[CacheScan] = None  # the scan that is in progress

    def add(self, name: str, size: int, last_access: float) -> None:
        """add (or replace) an entry as the most recently used one."""
        self.remove(name)
        self.entries[name] = (size, last_access)
        self.total += size

    def remove(self, name: str) -> None:
        entry = self.entries.pop(name, None)
        if entry is not None:
            self.total -= entry[0]
        if self.scan is not None:
            self.scan.changed.add(name)

    def replace_all(self, entries) -> None:
        """replace the contents by entries, an iterable of (name, size, last_access)."""
        self.entries = OrderedDict(
            (name, (size, last_access))
            for name, size, last_access in sorted(entries, key=lambda entry: (entry[2], entry[0]))
        )
        self.total = sum(size for size, _ in self.entries.values())

    def start_scan(self, infos: Generator[ItemInfo, None, None]) -> None:
        self.scan = CacheScan(infos)
        self.inserted = 0

    def abort_scan(self) -> None:
        if self.scan is not None:
            self.scan.infos.close()
            self.scan = None

    def finish_scan(self) -> tuple[int, int]:
        """
        Bring the index in line with what the scan has seen, return (added, removed) entry counts.

        The scan has listed the namespace while the store went on using the cache. It might have
        seen an item before the store removed it or have missed an item the store added after that
        directory was listed. Thus, for names the store has changed since the scan started, the
        index is right and the scan is ignored. For all other names, the scan is right:

        - known items the scan has not seen were evicted by another client.
        - unknown items the scan has seen were cached by another client.
        - for known items, the more recent one of our last access and the listed one counts
          (another client might have used the item).
        """
        scan, self.scan = self.scan, None
        gone = []  # names
        new, updated = [], []  # (name, (size, last_access))
        for name, (size, last_access) in self.entries.items():
            if name not in scan.changed:
                seen = scan.seen.get(name)
                if seen is None:
                    gone.append(name)
                elif seen[0] != size or seen[1] > last_access:
                    updated.append((name, (seen[0], max(last_access, seen[1]))))
        for name, seen in scan.seen.items():
            if name not in self.entries and name not in scan.changed:
                new.append((name, seen))
        for name in gone:
            self.remove(name)
        if new or updated:
            # these must be sorted in by their last access. if there are none (as usual if the
            # cache is not shared), the entries are in the right order already.
            entries = dict(self.entries)
            entries.update(new + updated)
            self.replace_all((name, size, last_access) for name, (size, last_access) in entries.items())
        return len(new), len(gone)


def get_backend(url, permissions=None, quota=None):
    """Parse backend URL and return a backend instance (or None)."""
    backend = get_file_backend(url, permissions=permissions, quota=quota)
    if backend is not None:
        return backend

    if permissions is not None:
        raise ValueError("Permissions are only supported for the 'file:' backend.")

    if quota is not None:
        raise ValueError("Quota is only supported for the 'file:' backend.")

    backend = get_sftp_backend(url)
    if backend is not None:
        return backend

    backend = get_rclone_backend(url)
    if backend is not None:
        return backend

    backend = get_s3_backend(url)
    if backend is not None:
        return backend

    backend = get_rest_backend(url)
    if backend is not None:
        return backend


def _locked(method):
    """Decorator: run the Store method while holding the store's lock, see Store docstring."""

    @wraps(method)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)

    return wrapper


class Store:
    """
    High-level key/value store, using a backend for the actual storage.

    Thread safety: a Store instance may be shared between threads, all operations are
    serialized by an internal lock (backends and the Store's own bookkeeping - stats,
    cache - are not thread-safe themselves, e.g. one sftp/rest session), #206.
    list() is special: it stays a lazy generator, the lock is only held while fetching
    the next item, so other threads' operations can interleave with a long listing
    (and the listing thread itself can do store operations inside its loop).
    Serialization is per operation - multi-operation sequences that need to be atomic
    against other threads must be coordinated by the caller.
    """

    def __init__(
        self,
        url: Optional[str] = None,
        backend: Optional[BackendBase] = None,
        config: Optional[dict] = None,
        permissions: Optional[dict] = None,
        *,
        cache_url: Optional[str] = None,
        cache_backend: Optional[BackendBase] = None,
    ):
        # serializes all operations of this store, see the class docstring.
        # reentrant, because operations nest (e.g. create_levels uses "with self:",
        # load/store/... call find).  created first: some @_locked methods run in __init__.
        self._lock = threading.RLock()
        self.url = url
        if backend is None and url is not None:
            backend = get_backend(url, permissions=permissions)
            if backend is None:
                raise BackendURLInvalid(f"Invalid or unsupported Backend Storage URL: {url}")
        if backend is None:
            raise NoBackendGiven("You need to give a backend instance or a backend url.")
        self.backend = backend
        if not config or not isinstance(config, dict):
            raise ValueError("No or invalid config given.")
        levels_dict = {}
        cache_policies = {}
        for namespace, ns_config in config.items():
            levels_list, policy = self._normalize_namespace_config(ns_config)
            levels_dict[namespace] = levels_list
            cache_policies[namespace] = policy
        self.set_levels(levels_dict)
        if cache_url is not None and cache_backend is not None:
            raise ValueError("Only one of cache_url and cache_backend can be given.")
        have_cache_enabled_namespaces = any(policy.mode != CacheMode.C_OFF for policy in cache_policies.values())
        if have_cache_enabled_namespaces and cache_url is None and cache_backend is None:
            raise ValueError("cache_url or cache_backend is required for cache modes other than C_OFF.")
        self.cache_backend = cache_backend if cache_backend is not None else None
        if self.cache_backend is None and cache_url is not None:
            self.cache_backend = get_backend(cache_url)
            if self.cache_backend is None:
                raise BackendURLInvalid(f"Invalid or unsupported Cache Backend URL: {cache_url}")
        self._cache_disabled = False
        # namespace -> CacheIndex, only for namespaces with a max_age or size limit, only while opened.
        self._cache_indexes: dict = {}
        self.cache_namespaces = [
            entry
            for entry in sorted(
                ((namespace, policy) for namespace, policy in cache_policies.items() if policy.mode != CacheMode.C_OFF),
                key=lambda item: len(item[0]),
                reverse=True,
            )
        ]
        self._stats: Counter = Counter()
        # this is to emulate additional latency to what the backend actually offers:
        self.latency = float(os.environ.get("BORGSTORE_LATENCY", "0")) / 1e6  # [us] -> [s]
        # this is to emulate less bandwidth than what the backend actually offers:
        self.bandwidth = float(os.environ.get("BORGSTORE_BANDWIDTH", "0")) / 8  # [bits/s] -> [bytes/s]

    def __repr__(self):
        backend = self.backend.__class__.__name__ if self.backend is not None else None
        if self.cache_backend is not None:
            cache_backend = self.cache_backend.__class__.__name__
            return f"<Store(backend={backend!r}, cache_backend={cache_backend!r})>"
        return f"<Store(backend={backend!r})>"

    @staticmethod
    def _normalize_namespace_config(ns_config: dict) -> tuple[list[int], "CachePolicy"]:
        """Parse a per-namespace config dict into (levels, CachePolicy)."""
        if not isinstance(ns_config, dict):
            raise ValueError(f"Invalid namespace config: expected a dict, got {type(ns_config).__name__!r}.")
        unknown_keys = set(ns_config) - {"levels", "cache", "max_age", "size"}
        if unknown_keys:
            raise ValueError(f"Invalid namespace config keys: {sorted(unknown_keys)!r}")
        levels = ns_config.get("levels")
        if not levels or not isinstance(levels, list):
            raise ValueError("'levels' is required and must be a non-empty list of ints.")
        cache_val = ns_config.get("cache")
        if cache_val is None:
            policy = CachePolicy(mode=CacheMode.C_OFF, max_age=None, size=None)
        else:
            mode = CacheMode.from_str(cache_val)
            max_age = ns_config.get("max_age")
            if max_age is not None:
                if not isinstance(max_age, (int, float)) or max_age < 0:
                    raise ValueError(f"Invalid cache max_age value: {max_age!r}")
                max_age = float(max_age)
            size = ns_config.get("size")
            if size is not None and (not isinstance(size, int) or size < 0):
                raise ValueError(f"Invalid cache size value: {size!r}")
            policy = CachePolicy(mode=mode, max_age=max_age, size=size)
        return levels, policy

    def _cache_policy_for(self, name: str) -> CachePolicy:
        for namespace, policy in self.cache_namespaces:
            if name.startswith(namespace):
                return policy
        return CachePolicy(mode=CacheMode.C_OFF, max_age=None, size=None)

    def _cache_index_for(self, name: str) -> Optional[CacheIndex]:
        for namespace, policy in self.cache_namespaces:
            if name.startswith(namespace):
                return self._cache_indexes.get(namespace)
        return None

    @_locked
    def set_levels(self, levels: dict, create: bool = False) -> None:
        if not levels or not isinstance(levels, dict):
            raise ValueError("No or invalid levels configuration given.")
        # we accept levels as a dict, but we rather want a list of (namespace, levels) tuples, longest namespace first:
        self.levels = [entry for entry in sorted(levels.items(), key=lambda item: len(item[0]), reverse=True)]
        if create:
            self.create_levels()

    @_locked
    def create_levels(self):
        """creating any needed namespaces / directory in advance"""
        # doing that saves a lot of ad-hoc mkdir calls, which is especially important
        # for backends with high latency or other noticeable costs of mkdir.
        with self:
            for namespace, levels in self.levels:
                namespace = namespace.rstrip("/")
                level = max(levels)
                cache_enabled = (
                    self.cache_backend is not None
                    and not self._cache_disabled
                    and self._cache_policy_for(f"{namespace}/").mode in {CacheMode.C_WRITETHROUGH, CacheMode.C_MIRROR}
                )
                if level == 0:
                    # flat, we just need to create the namespace directory:
                    self.backend.mkdir(namespace)
                    if cache_enabled:
                        self.cache_backend.mkdir(namespace)
                elif level > 0:
                    # nested, we only need to create the deepest nesting dir layer,
                    # any missing parent dirs will be created as needed by backend.mkdir.
                    limit = 2 ** (level * 8)
                    for i in range(limit):
                        dir = hexlify(i.to_bytes(length=level, byteorder="big")).decode("ascii")
                        name = f"{namespace}/{dir}" if namespace else dir
                        nested_name = nest(name, level)
                        self.backend.mkdir(nested_name[: -2 * level - 1])
                        if cache_enabled:
                            self.cache_backend.mkdir(nested_name[: -2 * level - 1])
                else:
                    raise ValueError(f"Invalid levels: {namespace}: {levels}")

    @_locked
    def create(self) -> None:
        self.backend.create()
        if self.cache_backend is not None and not self._cache_disabled:
            self.cache_backend.create()
        if self.backend.precreate_dirs:
            self.create_levels()

    @_locked
    def destroy(self) -> None:
        self.backend.destroy()
        if self.cache_backend is not None:
            self.cache_backend.destroy()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    @_locked
    def open(self) -> None:
        self.backend.open()
        if self.cache_backend is not None and not self._cache_disabled:
            try:
                self.cache_backend.open()
            except Exception as err:
                logger.warning(f"borgstore: cache open failed, disabling cache: {err!r}")
                self._cache_disabled = True
            else:
                self._cache_indexes = {
                    namespace: CacheIndex(namespace, policy)
                    for namespace, policy in self.cache_namespaces
                    if policy.max_age is not None or policy.size is not None
                }
                self._cache_cleanup()

    @_locked
    def close(self) -> None:
        self.backend.close()
        if self.cache_backend is not None:
            if not self._cache_disabled:
                self._cache_cleanup()
            self._cache_indexes = {}
            try:
                self.cache_backend.close()
            except Exception as err:
                logger.warning(f"borgstore: cache close failed: {err!r}")

    @_locked
    def quota(self) -> dict:
        return self.backend.quota()

    @contextmanager
    def _stats_updater(self, key, msg):
        """update call counters and overall times"""
        # do not use this in generators!
        volume_before = self._stats_get_volume(key)
        start = time.perf_counter_ns()
        yield
        end = time.perf_counter_ns()
        overall_time = end - start
        volume = self._stats_get_volume(key) - volume_before
        self._stats[f"{key}_calls"] += 1
        self._stats[f"{key}_time"] += overall_time
        logger.debug(f"borgstore: {msg} -> {volume}B in {overall_time / 1e6:0.1f}ms")

    def _backend_call(self, operation, *, key=None, volume=0):
        # latency and bandwidth emulation is only applied to (primary)
        # backend calls, not to (secondary) cache backend calls.
        if key is not None:
            self._stats[f"backend_{key}_calls"] += 1
        start = time.perf_counter_ns()
        result = operation()
        be_needed_ns = time.perf_counter_ns() - start
        volume = volume(result) if callable(volume) else volume
        if key is not None:
            self._stats[f"backend_{key}_volume"] += volume
        emulated_time = self.latency + (0 if not self.bandwidth else float(volume) / self.bandwidth)
        remaining_time = emulated_time - be_needed_ns / 1e9
        if remaining_time > 0.0:
            time.sleep(remaining_time)
        return result

    def _stats_update_volume(self, key, amount):
        self._stats[f"{key}_volume"] += amount

    def _stats_get_volume(self, key):
        return self._stats.get(f"{key}_volume", 0)

    @property
    @_locked
    def stats(self):
        """
        Return statistics such as method call counters, overall time [s], overall data volume, and overall throughput.

        Please note that the stats values only consider what is seen on the Store API:

        - There might be additional time spent by the caller, outside of Store, thus:

          - Real time is longer.
          - Real throughput is lower.
        - There are some overheads not accounted for, e.g., the volume only adds up the data size of load and store.
        - Write buffering or cached reads might give a wrong impression.
        """
        st = dict(self._stats)  # copy Counter -> generic dict
        for key in "info", "load", "store", "delete", "move", "list", "gather":
            # make sure key is present, even if method was not called
            st[f"{key}_calls"] = st.get(f"{key}_calls", 0)
            # convert integer ns timings to float s
            st[f"{key}_time"] = st.get(f"{key}_time", 0) / 1e9
        for key in "load", "store", "gather":
            v = st.get(f"{key}_volume", 0)
            t = st.get(f"{key}_time", 0)
            st[f"{key}_throughput"] = v / t if t else 0
        st["backend_load_calls"] = st.get("backend_load_calls", 0)
        st["backend_gather_calls"] = st.get("backend_gather_calls", 0)
        st["backend_store_calls"] = st.get("backend_store_calls", 0)
        st["backend_delete_calls"] = st.get("backend_delete_calls", 0)
        st["backend_load_volume"] = st.get("backend_load_volume", 0)
        st["backend_gather_volume"] = st.get("backend_gather_volume", 0)
        st["backend_store_volume"] = st.get("backend_store_volume", 0)
        st["cache_disabled"] = self._cache_disabled
        st["cache_hits"] = st.get("cache_hits", 0)
        st["cache_misses"] = st.get("cache_misses", 0)
        cache_total = st["cache_hits"] + st["cache_misses"]
        st["cache_hit_ratio"] = st["cache_hits"] / cache_total if cache_total else 0
        st["cache_errors"] = st.get("cache_errors", 0)
        st["cache_load_calls"] = st.get("cache_load_calls", 0)
        st["cache_store_calls"] = st.get("cache_store_calls", 0)
        st["cache_delete_calls"] = st.get("cache_delete_calls", 0)
        st["cache_load_volume"] = st.get("cache_load_volume", 0)
        st["cache_store_volume"] = st.get("cache_store_volume", 0)
        return st

    def _get_levels(self, name):
        """Get levels from the configuration depending on the namespace."""
        for namespace, levels in self.levels:
            if name.startswith(namespace):
                return levels
        # Store.create_levels requires all namespaces to be configured in self.levels.
        raise KeyError(f"no matching namespace found for: {name}")

    @_locked
    def find(self, name: str, *, deleted=False) -> str:
        """
        Find an item checking all supported nesting levels and return its nested name:

        - item not in the store yet:
          we won't find it, but find will return a nested name for **last** level.
        - item is in the store already:
          find will return the same nested name as the already present item.

        If deleted is True, find will try to find a "deleted" item.
        """
        nested_name = None
        suffix = DEL_SUFFIX if deleted else None
        levels = self._get_levels(name)
        if len(levels) == 1:
            # optimize the usual case:
            # the store is operating this namespace at a single specific level,
            # thus the item must be at that level, we do not need to search it.
            nested_name = nest(name, levels[0], add_suffix=suffix)
        else:
            # looks like the store is upgrading/downgrading levels,
            # items could be at old or new levels.
            for level in levels:
                nested_name = nest(name, level, add_suffix=suffix)
                info = self.backend.info(nested_name)
                if info.exists:
                    break
        return nested_name

    @_locked
    def info(self, name: str, *, deleted=False) -> ItemInfo:
        with self._stats_updater("info", f"info({name!r}, deleted={deleted})"):
            return self._backend_call(lambda: self.backend.info(self.find(name, deleted=deleted)), volume=0)

    def _cache_load(self, nested_name: str, *, size=None, offset=0) -> Optional[bytes]:
        if self.cache_backend is None or self._cache_disabled:
            return None
        self._stats["cache_load_calls"] += 1
        index = self._cache_index_for(nested_name)
        try:
            value = self.cache_backend.load(nested_name, size=size, offset=offset)
        except ObjectNotFound:
            self._stats["cache_misses"] += 1
            if index is not None:
                index.remove(nested_name)  # another client has evicted it
            return None
        except Exception as err:
            logger.warning(f"borgstore: cache load failed for {nested_name!r}: {err!r}")
            self._stats["cache_errors"] += 1
            return None
        self._stats["cache_hits"] += 1
        self._stats["cache_load_volume"] += len(value)
        if index is not None:
            entry = index.entries.get(nested_name)
            if entry is not None:
                item_size = entry[0]
            else:
                # another client has cached it. value might be only a part of the item.
                try:
                    item_size = self.cache_backend.info(nested_name).size
                except Exception:
                    item_size = len(value)
            index.add(nested_name, item_size, time.time())
        return value

    @_locked
    def load(self, name: str, *, size=None, offset=0, deleted=False) -> bytes:
        with self._stats_updater("load", f"load({name!r}, offset={offset}, size={size}, deleted={deleted})"):
            cache_policy = self._cache_policy_for(name)
            nested_name = self.find(name, deleted=deleted)
            if cache_policy.mode in {CacheMode.C_WRITETHROUGH, CacheMode.C_MIRROR}:
                result = self._cached_load(nested_name, cache_policy.mode, size=size, offset=offset)
            else:
                result = self._backend_call(
                    lambda: self.backend.load(nested_name, size=size, offset=offset),
                    key="load",
                    volume=lambda value: len(value),
                )
            self._stats_update_volume("load", len(result))
            return result

    def _cached_load(self, nested_name: str, mode: CacheMode, *, size=None, offset=0) -> bytes:
        """load (a range of) the value of an item in a cached namespace, see load."""
        if mode == CacheMode.C_WRITETHROUGH:
            # try a partial read from the cache first, matching the requested range.
            cached_value = self._cache_load(nested_name, size=size, offset=offset)
            if cached_value is not None:
                return cached_value
        # cache miss (or mirror mode): do a full load from the primary backend and populate the cache.
        full_value = self._backend_call(
            lambda: self.backend.load(nested_name, size=None, offset=0), key="load", volume=lambda value: len(value)
        )
        self._cache_store(nested_name, full_value)
        if offset < 0:
            # a negative offset counts from the end of the item. make it absolute, otherwise the end of
            # the slice (offset + size) is not right: a range ending at the end of the item would be empty.
            offset = max(len(full_value) + offset, 0)
        return full_value[offset : (None if size is None else offset + size)]

    @_locked
    def gather(self, sources, *, namespace=None, deleted=False) -> bytes:
        """
        read multiple byte ranges (from one or multiple items in the same namespace) and return
        their contents concatenated, in the order given.

        sources is a list of (name, offset, size) tuples, as for defrag: all items must be in the
        given namespace, item names are without namespace and must not contain "/". size must be
        given (an int), a short read raises ReadRangeError. the caller knows the sizes it requested,
        so it can split the result (e.g. into memoryview slices).

        a backend that supports it (e.g. rest) reads all the ranges with one roundtrip, while
        a partial load per range would cost one roundtrip each.
        """
        mapped_sources = self._find_sources(sources, namespace=namespace, deleted=deleted)
        with self._stats_updater(
            "gather", f"gather({len(mapped_sources)} ranges, namespace={namespace!r}, deleted={deleted})"
        ):
            prefix = (namespace + "/") if namespace else ""
            mode = self._cache_policy_for(prefix).mode
            if mode in {CacheMode.C_WRITETHROUGH, CacheMode.C_MIRROR}:
                # cached namespace: read the ranges like load does it (from the cache, or by
                # loading the whole item and caching it).
                parts = []
                for nested_name, offset, size in mapped_sources:
                    part = self._cached_load(nested_name, mode, size=size, offset=offset)
                    if len(part) != size:
                        raise ReadRangeError(
                            f"Read range error from {nested_name} "
                            f"(requested {size} bytes at offset {offset}, got {len(part)})"
                        )
                    parts.append(part)
                result = b"".join(parts)
            elif mapped_sources:
                # gather all ranges from the backend with one call.
                result = self._backend_call(
                    lambda: self.backend.gather(mapped_sources), key="gather", volume=lambda value: len(value)
                )
                expected_size = sum(size for _, _, size in mapped_sources)
                if len(result) != expected_size:
                    raise ReadRangeError(
                        f"Read range error: gather returned {len(result)} bytes, expected {expected_size}"
                    )
            else:
                result = b""
            self._stats_update_volume("gather", len(result))
            return result

    def _find_sources(self, sources, *, namespace, deleted) -> list:
        """
        validate the sources of gather / defrag and return them with the nested (backend) item names.

        all items must be in the given namespace: a "/" in an item name is rejected, because such an
        item could be in a deeper namespace (with other nesting levels and cache policy).
        """
        sources = validate_sources(sources)
        prefix = (namespace + "/") if namespace else ""
        nested_names: dict[str, str] = {}
        mapped_sources = []
        for name, offset, size in sources:
            if name not in nested_names:
                if "/" in name:
                    raise ValueError(f"item name must not contain '/' (namespace is given separately): {name!r}")
                nested_names[name] = self.find(prefix + name, deleted=deleted)
            mapped_sources.append((nested_names[name], offset, size))
        return mapped_sources

    def _cache_store(self, nested_name: str, value: StoreValue) -> None:
        if self.cache_backend is None or self._cache_disabled:
            return
        index = self._cache_index_for(nested_name)
        if index is not None:
            size_limit = index.policy.size
            if size_limit is not None and len(value) > size_limit:
                # it can never fit. also make sure the cache does not keep a previous value.
                self._cache_delete(nested_name)
                return
            if index.scan is not None or (
                size_limit is not None and index.inserted > size_limit / CACHE_RESCAN_DIVISOR
            ):
                self._cache_scan(index, max_time=CACHE_SCAN_STEP_TIME)
            # make room before storing, so the cache does not exceed its size limit.
            # the value replaces a previous one (if any), so that does not count.
            index.remove(nested_name)
            self._cache_evict(index, needed=len(value))
        self._stats["cache_store_calls"] += 1
        try:
            self.cache_backend.store(nested_name, value)
            self._stats["cache_store_volume"] += len(value)
        except Exception as err:
            logger.warning(f"borgstore: cache store failed for {nested_name!r}: {err!r}")
            self._stats["cache_errors"] += 1
        else:
            if index is not None:
                index.add(nested_name, len(value), time.time())
                index.inserted += len(value)

    @_locked
    def store(self, name: str, value: StoreValue) -> None:
        """
        store <value> into item <name>.

        <value> is either bytes or a memoryview of bytes - a memoryview enables callers
        to avoid copying (e.g. by giving a slice of a bigger buffer they already have).
        The value is only used while this method runs, so the caller may reuse or
        release the underlying buffer afterwards.
        """
        # note: using .find here will:
        # - overwrite an existing item (level stays same)
        # - write to the last level if no existing item is found.
        value = validate_value(value)  # normalize, so len(value) is the number of bytes
        with self._stats_updater("store", f"store({name!r})"):
            nested_name = self.find(name)
            self._backend_call(lambda: self.backend.store(nested_name, value), key="store", volume=len(value))
            if self._cache_policy_for(name).mode in {CacheMode.C_WRITETHROUGH, CacheMode.C_MIRROR}:
                self._cache_store(nested_name, value)
            self._stats_update_volume("store", len(value))

    def _cache_delete(self, nested_name: str) -> None:
        if self.cache_backend is None or self._cache_disabled:
            return
        self._stats["cache_delete_calls"] += 1
        index = self._cache_index_for(nested_name)
        if index is not None:
            # also if deleting fails: the eviction must not try the same item again and again,
            # the next scan brings back an item that is still there.
            index.remove(nested_name)
        try:
            self.cache_backend.delete(nested_name)
        except ObjectNotFound:
            pass
        except Exception as err:
            logger.warning(f"borgstore: cache delete failed for {nested_name!r}: {err!r}")
            self._stats["cache_errors"] += 1

    @_locked
    def delete(self, name: str, *, deleted=False) -> None:
        """
        Really and immediately deletes an item.

        See also .move(name, delete=True) for "soft" deletion.
        """
        with self._stats_updater("delete", f"delete({name!r}, deleted={deleted})"):
            nested_name = self.find(name, deleted=deleted)
            self._backend_call(lambda: self.backend.delete(nested_name), key="delete", volume=0)
            if self._cache_policy_for(name).mode in {CacheMode.C_WRITETHROUGH, CacheMode.C_MIRROR}:
                self._cache_delete(nested_name)

    @_locked
    def cache_invalidate(self, name: str, *, deleted: bool = False) -> None:
        """
        Invalidate cached items.

        - If name is ROOTNS (""), invalidate caches of all cached namespaces.
        - If a namespace is given, invalidate all items in that namespace.
        - If an item name is given, invalidate only that single item.
        """
        if self.cache_backend is None or self._cache_disabled:
            return

        if name == ROOTNS:
            # Root / all namespaces
            for namespace, policy in self.cache_namespaces:
                for info in self._cache_list(namespace.rstrip("/")):
                    if not info.directory:
                        self._cache_delete(info.name)
        else:
            # Check if name represents a namespace
            target_namespace = None
            for namespace, policy in self.cache_namespaces:
                if namespace.rstrip("/") == name.rstrip("/"):
                    target_namespace = namespace
                    break

            if target_namespace is not None:
                # Invalidate all items in the namespace
                for info in self._cache_list(target_namespace.rstrip("/")):
                    if not info.directory:
                        self._cache_delete(info.name)
            else:
                # Invalidate single item
                nested_name = self.find(name, deleted=deleted)
                self._cache_delete(nested_name)

    def _cache_move(self, old_nested: str, new_nested: str) -> None:
        if self.cache_backend is None or self._cache_disabled:
            return
        old_index = self._cache_index_for(old_nested)
        entry = old_index.entries.get(old_nested) if old_index is not None else None
        try:
            self.cache_backend.move(old_nested, new_nested)
        except ObjectNotFound:
            if old_index is not None:
                old_index.remove(old_nested)  # another client has evicted it
        except Exception as err:
            logger.warning(f"borgstore: cache move failed for {old_nested!r}->{new_nested!r}: {err!r}")
            self._stats["cache_errors"] += 1
        else:
            if old_index is not None:
                old_index.remove(old_nested)
            new_index = self._cache_index_for(new_nested)
            if new_index is not None:
                if entry is not None:
                    new_index.add(new_nested, entry[0], time.time())  # moving counts as using it
                else:
                    new_index.remove(new_nested)  # unknown item, the next scan or cache hit adds it

    @_locked
    def move(
        self,
        name: str,
        new_name: Optional[str] = None,
        *,
        delete: bool = False,
        undelete: bool = False,
        change_level: bool = False,
        deleted: bool = False,
    ) -> None:
        if delete:
            # use case: keep name, but soft "delete" the item
            nested_name = self.find(name, deleted=False)
            nested_new_name = nested_name + DEL_SUFFIX
            msg = f"soft_delete({name!r}, deleted={deleted})"
        elif undelete:
            # use case: keep name, undelete a previously soft "deleted" item
            nested_name = self.find(name, deleted=True)
            nested_new_name = nested_name.removesuffix(DEL_SUFFIX)
            msg = f"soft_undelete({name!r}, deleted={deleted})"
        elif change_level:
            # use case: keep name, changing to another nesting level
            suffix = DEL_SUFFIX if deleted else None
            nested_name = self.find(name, deleted=deleted)
            nested_new_name = nest(name, self._get_levels(name)[-1], add_suffix=suffix)
            msg = f"change_level({name!r}, deleted={deleted})"
        else:
            # generic use (be careful!)
            if not new_name:
                raise ValueError("Generic move requires new_name to be given.")
            nested_name = self.find(name, deleted=deleted)
            nested_new_name = self.find(new_name, deleted=deleted)
            msg = f"rename({name!r}, {new_name!r}, deleted={deleted})"
        with self._stats_updater("move", msg + f" [{nested_name!r}, {nested_new_name!r}]"):
            self._backend_call(lambda: self.backend.move(nested_name, nested_new_name), volume=0)
            if self._cache_policy_for(name).mode in {CacheMode.C_WRITETHROUGH, CacheMode.C_MIRROR}:
                self._cache_move(nested_name, nested_new_name)

    def _cache_list(self, name: str, *, dirs: bool = False) -> Generator[ItemInfo, None, None]:
        """list all cached items below <name> (recursively), with dirs=True also the directories."""
        if self.cache_backend is None:
            return
        for info in self.cache_backend.list(name):
            if info.directory:
                subdir_name = (name + "/" + info.name) if name else info.name
                if dirs:
                    yield info._replace(name=subdir_name)
                yield from self._cache_list(subdir_name, dirs=dirs)
            else:
                full_name = (name + "/" + info.name) if name else info.name
                yield info._replace(name=full_name)

    def list(self, name: str, deleted: bool = False) -> Iterator[ItemInfo]:
        """
        List all names in the namespace <name>.

        If deleted is False (default), only non-deleted items are yielded.
        If deleted is True, only soft-deleted items are yielded.

        backend.list giving us sorted names implies Store.list is also sorted,
        if all items are stored on the same level.

        Note: list bypasses the cache and always queries the primary backend to ensure we
        only return items that really exist there, even if other clients have updated or
        deleted items directly in the primary backend.

        Note: the store's lock is only held while fetching the next item, not across the
        whole iteration, so other threads' operations (and the iterating thread's own
        operations inside its loop) interleave with a long listing, see the class docstring.
        """
        # we need this wrapper due to the recursion - we only want to increment list_calls once:
        logger.debug(f"borgstore: list_start({name!r}, deleted={deleted})")
        with self._lock:
            self._stats["list_calls"] += 1
            inner = self._list(name, deleted=deleted)
        count = 0
        try:
            while True:
                with self._lock:
                    try:
                        info = next(inner)
                    except StopIteration:
                        break
                count += 1
                yield info
        finally:
            # note: as this is a generator, we do not measure the execution time because
            # that would include the time needed by the caller to process the infos.
            logger.debug(f"borgstore: list_end({name!r}, deleted={deleted}) -> {count}")

    def _list(self, name: str, deleted: bool = False) -> Iterator[ItemInfo]:
        # as the backend.list method only supports non-recursive listing and
        # also returns directories/namespaces we introduced for nesting, we do the
        # recursion here (and also we do not yield directory names from here).
        start = time.perf_counter_ns()
        backend_list_iterator = self.backend.list(name)
        if self.latency:
            # we add the simulated latency once per backend.list iteration, not per element.
            time.sleep(self.latency)
        end = time.perf_counter_ns()
        self._stats["list_time"] += end - start
        while True:
            start = time.perf_counter_ns()
            try:
                info = next(backend_list_iterator)
            except StopIteration:
                break
            finally:
                end = time.perf_counter_ns()
                self._stats["list_time"] += end - start
            if info.directory:
                # note: we only expect subdirectories from key nesting, but not namespaces nested into each other.
                subdir_name = (name + "/" + info.name) if name else info.name
                yield from self._list(subdir_name, deleted=deleted)
            else:
                is_deleted = info.name.endswith(DEL_SUFFIX)
                if deleted and is_deleted:
                    yield info._replace(name=info.name.removesuffix(DEL_SUFFIX))
                elif not deleted and not is_deleted:
                    yield info

    @_locked
    def hash(self, name: str, algorithm: str = "sha256", *, deleted: bool = False) -> str:
        """
        compute the hex digest of the content of item <name> using <algorithm>.

        algorithm can be any algorithm supported by hashlib (e.g. "sha256") or "blake3".
        blake3 needs the optional "blake3" package to be installed - for backends that
        compute the hash remotely, it must be installed on the server side.
        """
        with self._stats_updater("hash", f"hash({name!r}, algorithm={algorithm!r}, deleted={deleted})"):
            return self._backend_call(
                lambda: self.backend.hash(self.find(name, deleted=deleted), algorithm=algorithm), volume=0
            )

    @_locked
    def defrag(self, sources, *, target=None, algorithm=None, namespace=None, deleted=False) -> str:
        """
        efficiently create a new item (target) by combining blocks from existing items (sources)
        in the same namespace. all items must be in the given namespace, item and target names are
        without namespace and must not contain "/".

        sources is a list of (name, block_offset, block_length) tuples. blocks will be processed
        in order of appearance in the list and their contents will be appended to the target item.

        if the target name is not given, algorithm must be given to compute the target name
        as hash(algorithm, target_content).hexdigest(). the supported algorithms are the same
        as for the hash method, see there.

        returns the target name.
        """
        prefix = (namespace + "/") if namespace else ""
        mapped_sources = self._find_sources(sources, namespace=namespace, deleted=deleted)
        if target is not None:
            if "/" in target:
                raise ValueError(f"item name must not contain '/' (namespace is given separately): {target!r}")
            target = self.find(prefix + target, deleted=deleted)

        # Note: defrag does not interact with the cache. It creates a new item from
        # the chunks of the source items we want to keep. It does not delete the source
        # items; that is the task of the caller after defrag successfully returns the new
        # item name. If the caller subsequently deletes the source items, they will be
        # removed from the cache.
        levels = self._get_levels(prefix)[-1] if prefix else 0
        backend_target = self.backend.defrag(
            mapped_sources, target=target, algorithm=algorithm, namespace=prefix.rstrip("/"), levels=levels
        )
        return unnest(backend_target, namespace=prefix).removeprefix(prefix)

    def _cache_scan(self, index: CacheIndex, *, max_time: Optional[float] = None) -> None:
        """
        Bring index in line with what the cache backend really has in that namespace:
        other clients sharing the cache add, use and evict items, too.

        Scanning means listing the whole namespace, which takes long if it has a lot of items.
        If max_time [s] is given, the scan is done in steps: a call starts a scan or continues
        the scan that is in progress for about that time, the call that gets to the end of the
        listing finishes the scan and updates the index, see CacheIndex.finish_scan.
        Between the steps, the store uses the cache and the index as usual.
        """
        started = time.perf_counter()
        namespace = index.namespace
        if index.scan is None:
            # this also resets index.inserted, so a failing scan is not retried for each store operation.
            index.start_scan(self._cache_list(namespace.rstrip("/"), dirs=True))
        scan = index.scan
        scan.steps += 1
        finished = False
        try:
            # a step lists at least one item or directory, so the scan makes progress.
            for info in scan.infos:
                if not info.directory:
                    # if the backend has no atime, the mtime (when the item was cached) is the best guess.
                    scan.seen[info.name] = (info.size, info.atime or info.mtime)
                if max_time is not None and time.perf_counter() - started >= max_time:
                    break
            else:
                finished = True
        except ObjectNotFound:
            # nothing was cached in this namespace yet (or a directory vanished while listing).
            finished = True
        except Exception as err:
            logger.warning(f"borgstore: cache scan failed for namespace {namespace!r}: {err!r}")
            self._stats["cache_errors"] += 1
            index.abort_scan()
            return
        if finished:
            added, removed = index.finish_scan()
        scan.time += time.perf_counter() - started
        if finished:
            logger.debug(
                f"borgstore: cache scan of namespace {namespace!r} -> {len(index.entries)} items "
                f"({added} new, {removed} gone) in {scan.time * 1e3:0.1f}ms ({scan.steps} steps)"
            )

    def _cache_evict(self, index: CacheIndex, *, needed: int = 0) -> None:
        """
        Evict items that are older than max_age, then evict least recently used items until
        <needed> more bytes fit into the size limit.
        """
        policy = index.policy
        if policy.max_age is not None:
            now = time.time()
            while index.entries:
                name, (size, last_access) = next(iter(index.entries.items()))
                # last_access is 0 if the item is only known from a backend that has neither atime nor mtime.
                if last_access and (now - last_access) <= policy.max_age:
                    break
                self._cache_delete(name)  # also removes it from the index
        if policy.size is not None:
            while index.entries and index.total + needed > policy.size:
                self._cache_delete(next(iter(index.entries)))  # also removes it from the index

    def _cache_cleanup(self) -> None:
        for index in self._cache_indexes.values():
            index.abort_scan()  # a scan that was done in steps does not know the latest changes by other clients
            self._cache_scan(index)
            self._cache_evict(index)
