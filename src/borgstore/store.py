"""
Key/value store implementation.

The Store uses a backend to store key/value data and adds some functionality:

- backend creation from a URL
- configurable nesting
- recursive list method
- soft deletion
"""

from binascii import hexlify
from collections import Counter
from contextlib import contextmanager
import enum
import logging
import os
import time
from typing import Iterator, NamedTuple, Optional

from .utils.nesting import nest, unnest
from .backends._base import ItemInfo, BackendBase
from .backends.errors import ObjectNotFound, NoBackendGiven, BackendURLInvalid  # noqa
from .backends.posixfs import get_file_backend
from .backends.rclone import get_rclone_backend
from .backends.sftp import get_sftp_backend
from .backends.s3 import get_s3_backend
from .backends.rest import get_rest_backend
from .constants import DEL_SUFFIX

logger = logging.getLogger(__name__)


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


class Store:
    def __init__(
        self,
        url: Optional[str] = None,
        backend: Optional[BackendBase] = None,
        levels: Optional[dict] = None,
        permissions: Optional[dict] = None,
        *,
        cache: Optional[dict[str, CachePolicy | dict]] = None,
        cache_url: Optional[str] = None,
        cache_backend: Optional[BackendBase] = None,
    ):
        self.url = url
        if backend is None and url is not None:
            backend = get_backend(url, permissions=permissions)
            if backend is None:
                raise BackendURLInvalid(f"Invalid or unsupported Backend Storage URL: {url}")
        if backend is None:
            raise NoBackendGiven("You need to give a backend instance or a backend url.")
        self.backend = backend
        self.set_levels(levels)
        if cache_url is not None and cache_backend is not None:
            raise ValueError("Only one of cache_url and cache_backend can be given.")
        cache = cache or {}
        if not isinstance(cache, dict):
            raise ValueError("Invalid cache configuration: expected a dict mapping namespace to policy.")
        cache_policies = {namespace: self._normalize_cache_policy(policy) for namespace, policy in cache.items()}
        configured_namespaces = {namespace for namespace, _ in self.levels}
        for namespace, policy in cache_policies.items():
            if policy.mode != CacheMode.C_OFF and namespace not in configured_namespaces:
                raise ValueError(f"Invalid cache namespace configuration: {namespace!r} not in levels.")
        have_cache_enabled_namespaces = any(policy.mode != CacheMode.C_OFF for policy in cache_policies.values())
        if have_cache_enabled_namespaces and cache_url is None and cache_backend is None:
            raise ValueError("cache_url or cache_backend is required for cache modes other than C_OFF.")
        self.cache_backend = cache_backend if cache_backend is not None else None
        if self.cache_backend is None and cache_url is not None:
            self.cache_backend = get_backend(cache_url)
            if self.cache_backend is None:
                raise BackendURLInvalid(f"Invalid or unsupported Cache Backend URL: {cache_url}")
        self._cache_disabled = False
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
        if self.cache_backend is not None or self.cache_namespaces:
            cache_backend = self.cache_backend.__class__.__name__ if self.cache_backend is not None else None
            return (
                f"<Store(url={self.url!r}, levels={self.levels!r}, "
                f"cache_namespaces={self.cache_namespaces!r}, cache_backend={cache_backend!r})>"
            )
        return f"<Store(url={self.url!r}, levels={self.levels!r})>"

    @staticmethod
    def _normalize_cache_policy(policy: CachePolicy | dict) -> CachePolicy:
        if isinstance(policy, CachePolicy):
            return policy
        if isinstance(policy, dict):
            unknown_keys = set(policy) - {"mode", "max_age", "size"}
            if unknown_keys:
                raise ValueError(f"Invalid cache policy keys: {sorted(unknown_keys)!r}")
            if "mode" not in policy:
                raise ValueError("Invalid cache policy: 'mode' is required.")
            mode = CacheMode.from_str(policy["mode"])
            max_age = policy.get("max_age")
            if max_age is not None:
                if not isinstance(max_age, (int, float)) or max_age < 0:
                    raise ValueError(f"Invalid cache max_age value: {max_age!r}")
                max_age = float(max_age)
            size = policy.get("size")
            if size is not None and (not isinstance(size, int) or size < 0):
                raise ValueError(f"Invalid cache size value: {size!r}")
            return CachePolicy(mode=mode, max_age=max_age, size=size)
        raise ValueError("Invalid cache policy: expected dict or CachePolicy.")

    def _cache_policy_for(self, name: str) -> CachePolicy:
        for namespace, policy in self.cache_namespaces:
            if name.startswith(namespace):
                return policy
        return CachePolicy(mode=CacheMode.C_OFF, max_age=None, size=None)

    def _cache_is_expired(self, nested_name: str, max_age: Optional[float]) -> bool:
        if max_age is None:
            return False
        if self.cache_backend is None or self._cache_disabled:
            return True
        try:
            info = self.cache_backend.info(nested_name)
        except ObjectNotFound:
            return True
        except Exception as err:
            logger.warning(f"borgstore: cache info failed for {nested_name!r}: {err!r}")
            self._stats["cache_errors"] += 1
            return True
        if not info.atime:
            return True
        return (time.time() - info.atime) > max_age

    def set_levels(self, levels: dict, create: bool = False) -> None:
        if not levels or not isinstance(levels, dict):
            raise ValueError("No or invalid levels configuration given.")
        # we accept levels as a dict, but we rather want a list of (namespace, levels) tuples, longest namespace first:
        self.levels = [entry for entry in sorted(levels.items(), key=lambda item: len(item[0]), reverse=True)]
        if create:
            self.create_levels()

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

    def create(self) -> None:
        self.backend.create()
        if self.cache_backend is not None and not self._cache_disabled:
            self.cache_backend.create()
        if self.backend.precreate_dirs:
            self.create_levels()

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

    def open(self) -> None:
        self.backend.open()
        if self.cache_backend is not None and not self._cache_disabled:
            try:
                self.cache_backend.open()
            except Exception as err:
                logger.warning(f"borgstore: cache open failed, disabling cache: {err!r}")
                self._cache_disabled = True

    def _cache_list(self, name: str) -> Iterator[ItemInfo]:
        if self.cache_backend is None:
            return
        for info in self.cache_backend.list(name):
            if info.directory:
                subdir_name = (name + "/" + info.name) if name else info.name
                yield from self._cache_list(subdir_name)
            else:
                full_name = (name + "/" + info.name) if name else info.name
                yield info._replace(name=full_name)

    def _cache_cleanup_expired(self) -> None:
        now = time.time()
        for namespace, policy in self.cache_namespaces:
            if policy.max_age is None and policy.size is None:
                continue
            try:
                items = [info for info in self._cache_list(namespace.rstrip("/")) if not info.directory]
                if policy.max_age is not None:
                    remaining_items = []
                    for info in items:
                        if not info.atime or (now - info.atime) > policy.max_age:
                            self._cache_invalidate(info.name)
                        else:
                            remaining_items.append(info)
                    items = remaining_items
                if policy.size is not None:
                    total_size = sum(info.size for info in items)
                    for info in sorted(items, key=lambda entry: (entry.atime, entry.name)):
                        if total_size <= policy.size:
                            break
                        self._cache_invalidate(info.name)
                        total_size -= info.size
            except Exception as err:
                logger.warning(f"borgstore: cache cleanup failed for namespace {namespace!r}: {err!r}")
                self._stats["cache_errors"] += 1

    def close(self) -> None:
        self.backend.close()
        if self.cache_backend is not None:
            self._cache_cleanup_expired()
            try:
                self.cache_backend.close()
            except Exception as err:
                logger.warning(f"borgstore: cache close failed: {err!r}")

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

    def _backend_call(self, operation, *, volume=0):
        # latency and bandwidth emulation is only applied to (primary)
        # backend calls, not to (secondary) cache backend calls.
        start = time.perf_counter_ns()
        result = operation()
        be_needed_ns = time.perf_counter_ns() - start
        volume = volume(result) if callable(volume) else volume
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
        for key in "info", "load", "store", "delete", "move", "list":
            # make sure key is present, even if method was not called
            st[f"{key}_calls"] = st.get(f"{key}_calls", 0)
            # convert integer ns timings to float s
            st[f"{key}_time"] = st.get(f"{key}_time", 0) / 1e9
        for key in "load", "store":
            v = st.get(f"{key}_volume", 0)
            t = st.get(f"{key}_time", 0)
            st[f"{key}_throughput"] = v / t if t else 0
        st["cache_hits"] = st.get("cache_hits", 0)
        st["cache_misses"] = st.get("cache_misses", 0)
        st["cache_errors"] = st.get("cache_errors", 0)
        st["cache_bytes_read"] = st.get("cache_bytes_read", 0)
        st["cache_bytes_written"] = st.get("cache_bytes_written", 0)
        st["cache_disabled"] = self._cache_disabled
        cache_total = st["cache_hits"] + st["cache_misses"]
        st["cache_hit_ratio"] = st["cache_hits"] / cache_total if cache_total else 0
        return st

    def _cache_get(self, nested_name: str, *, max_age: Optional[float] = None) -> Optional[bytes]:
        if self.cache_backend is None or self._cache_disabled:
            return None
        if self._cache_is_expired(nested_name, max_age):
            self._cache_invalidate(nested_name)
            self._stats["cache_misses"] += 1
            return None
        try:
            value = self.cache_backend.load(nested_name)
        except ObjectNotFound:
            self._stats["cache_misses"] += 1
            return None
        except Exception as err:
            logger.warning(f"borgstore: cache load failed for {nested_name!r}: {err!r}")
            self._stats["cache_errors"] += 1
            return None
        self._stats["cache_hits"] += 1
        self._stats["cache_bytes_read"] += len(value)
        return value

    def _cache_put(self, nested_name: str, value: bytes) -> None:
        if self.cache_backend is None or self._cache_disabled:
            return
        try:
            self.cache_backend.store(nested_name, value)
            self._stats["cache_bytes_written"] += len(value)
        except Exception as err:
            logger.warning(f"borgstore: cache store failed for {nested_name!r}: {err!r}")
            self._stats["cache_errors"] += 1

    def _cache_invalidate(self, nested_name: str) -> None:
        if self.cache_backend is None or self._cache_disabled:
            return
        try:
            self.cache_backend.delete(nested_name)
        except ObjectNotFound:
            pass
        except Exception as err:
            logger.warning(f"borgstore: cache delete failed for {nested_name!r}: {err!r}")
            self._stats["cache_errors"] += 1

    def _cache_move(self, old_nested: str, new_nested: str) -> None:
        if self.cache_backend is None or self._cache_disabled:
            return
        try:
            self.cache_backend.move(old_nested, new_nested)
        except ObjectNotFound:
            pass
        except Exception as err:
            logger.warning(f"borgstore: cache move failed for {old_nested!r}->{new_nested!r}: {err!r}")
            self._stats["cache_errors"] += 1

    def _get_levels(self, name):
        """Get levels from the configuration depending on the namespace."""
        for namespace, levels in self.levels:
            if name.startswith(namespace):
                return levels
        # Store.create_levels requires all namespaces to be configured in self.levels.
        raise KeyError(f"no matching namespace found for: {name}")

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

    def info(self, name: str, *, deleted=False) -> ItemInfo:
        with self._stats_updater("info", f"info({name!r}, deleted={deleted})"):
            return self._backend_call(lambda: self.backend.info(self.find(name, deleted=deleted)), volume=0)

    def load(self, name: str, *, size=None, offset=0, deleted=False) -> bytes:
        with self._stats_updater("load", f"load({name!r}, offset={offset}, size={size}, deleted={deleted})"):
            cache_policy = self._cache_policy_for(name)
            nested_name = self.find(name, deleted=deleted)
            if cache_policy.mode == CacheMode.C_WRITETHROUGH:
                full_value = self._cache_get(nested_name, max_age=cache_policy.max_age)
                if full_value is None:
                    full_value = self._backend_call(
                        lambda: self.backend.load(nested_name, size=None, offset=0), volume=lambda value: len(value)
                    )
                    self._cache_put(nested_name, full_value)
            elif cache_policy.mode == CacheMode.C_MIRROR:
                full_value = self._backend_call(
                    lambda: self.backend.load(nested_name, size=None, offset=0), volume=lambda value: len(value)
                )
                self._cache_put(nested_name, full_value)
            else:
                result = self._backend_call(
                    lambda: self.backend.load(nested_name, size=size, offset=offset), volume=lambda value: len(value)
                )
                self._stats_update_volume("load", len(result))
                return result
            result = full_value[offset : (None if size is None else offset + size)]
            self._stats_update_volume("load", len(result))
            return result

    def store(self, name: str, value: bytes) -> None:
        # note: using .find here will:
        # - overwrite an existing item (level stays same)
        # - write to the last level if no existing item is found.
        with self._stats_updater("store", f"store({name!r})"):
            nested_name = self.find(name)
            self._backend_call(lambda: self.backend.store(nested_name, value), volume=len(value))
            if self._cache_policy_for(name).mode in {CacheMode.C_WRITETHROUGH, CacheMode.C_MIRROR}:
                self._cache_put(nested_name, value)
            self._stats_update_volume("store", len(value))

    def hash(self, name: str, algorithm: str = "sha256", *, deleted: bool = False) -> str:
        with self._stats_updater("hash", f"hash({name!r}, algorithm={algorithm!r}, deleted={deleted})"):
            return self._backend_call(
                lambda: self.backend.hash(self.find(name, deleted=deleted), algorithm=algorithm), volume=0
            )

    def delete(self, name: str, *, deleted=False) -> None:
        """
        Really and immediately deletes an item.

        See also .move(name, delete=True) for "soft" deletion.
        """
        with self._stats_updater("delete", f"delete({name!r}, deleted={deleted})"):
            nested_name = self.find(name, deleted=deleted)
            self._backend_call(lambda: self.backend.delete(nested_name), volume=0)
            if self._cache_policy_for(name).mode in {CacheMode.C_WRITETHROUGH, CacheMode.C_MIRROR}:
                self._cache_invalidate(nested_name)

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

    def defrag(self, sources, *, target=None, algorithm=None, namespace=None, deleted=False) -> str:
        """
        efficiently create a new item (target) by combining blocks from existing items (sources)
        in the same namespace. item and target names are always without namespace.

        sources is a list of (name, block_offset, block_length) tuples. blocks will be processed
        in order of appearance in the list and their contents will be appended to the target item.

        if the target name is not given, algorithm must be given to compute the target name
        as hash(algorithm, target_content).hexdigest().

        returns the target name.
        """
        prefix = (namespace + "/") if namespace else ""
        mapped_sources = [
            (self.find(prefix + source, deleted=deleted), offset, size) for source, offset, size in sources
        ]
        if target is not None:
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
        """
        # we need this wrapper due to the recursion - we only want to increment list_calls once:
        logger.debug(f"borgstore: list_start({name!r}, deleted={deleted})")
        self._stats["list_calls"] += 1
        count = 0
        try:
            for info in self._list(name, deleted=deleted):
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
