"""
Key/Value Store Implementation.

Store internally uses a backend to store k/v data and adds some functionality:

- backend creation from a URL
- configurable nesting
- recursive .list method
- soft deletion
"""
from binascii import hexlify
from collections import Counter
from contextlib import contextmanager
import os
import time
from typing import Iterator, Optional

from .utils.nesting import nest
from .backends._base import ItemInfo, BackendBase
from .backends.errors import ObjectNotFound, NoBackendGiven, BackendURLInvalid  # noqa
from .backends.posixfs import get_file_backend
from .backends.rclone import get_rclone_backend
from .backends.sftp import get_sftp_backend
from .constants import DEL_SUFFIX


def get_backend(url):
    """parse backend URL and return a backend instance (or None)"""
    backend = get_file_backend(url)
    if backend is not None:
        return backend

    backend = get_sftp_backend(url)
    if backend is not None:
        return backend

    backend = get_rclone_backend(url)
    if backend is not None:
        return backend


class Store:
    def __init__(self, url: Optional[str] = None, backend: Optional[BackendBase] = None, levels: Optional[dict] = None):
        self.url = url
        if backend is None and url is not None:
            backend = get_backend(url)
            if backend is None:
                raise BackendURLInvalid(f"Invalid Backend Storage URL: {url}")
        if backend is None:
            raise NoBackendGiven("You need to give a backend instance or a backend url.")
        self.backend = backend
        self.set_levels(levels)
        self._stats: Counter = Counter()
        # this is to emulate additional latency to what the backend actually offers:
        self.latency = float(os.environ.get("BORGSTORE_LATENCY", "0")) / 1e6  # [us] -> [s]
        # this is to emulate less bandwidth than what the backend actually offers:
        self.bandwidth = float(os.environ.get("BORGSTORE_BANDWIDTH", "0")) / 8  # [bits/s] -> [bytes/s]

    def __repr__(self):
        return f"<Store(url={self.url!r}, levels={self.levels!r})>"

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
                if level == 0:
                    # flat, we just need to create the namespace directory:
                    self.backend.mkdir(namespace)
                elif level > 0:
                    # nested, we only need to create the deepest nesting dir layer,
                    # any missing parent dirs will be created as needed by backend.mkdir.
                    limit = 2 ** (level * 8)
                    for i in range(limit):
                        dir = hexlify(i.to_bytes(length=level, byteorder="big")).decode("ascii")
                        name = f"{namespace}/{dir}" if namespace else dir
                        nested_name = nest(name, level)
                        self.backend.mkdir(nested_name[: -2 * level - 1])
                else:
                    raise ValueError(f"Invalid levels: {namespace}: {levels}")

    def create(self) -> None:
        self.backend.create()
        if self.backend.precreate_dirs:
            self.create_levels()

    def destroy(self) -> None:
        self.backend.destroy()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def open(self) -> None:
        self.backend.open()

    def close(self) -> None:
        self.backend.close()

    @contextmanager
    def _stats_updater(self, key):
        """update call counters and overall times, also emulate latency and bandwidth"""
        # do not use this in generators!
        volume_before = self._stats_get_volume(key)
        start = time.perf_counter_ns()
        yield
        be_needed_ns = time.perf_counter_ns() - start
        volume_after = self._stats_get_volume(key)
        volume = volume_after - volume_before
        emulated_time = self.latency + (0 if not self.bandwidth else float(volume) / self.bandwidth)
        remaining_time = emulated_time - be_needed_ns / 1e9
        if remaining_time > 0.0:
            time.sleep(remaining_time)
        end = time.perf_counter_ns()
        self._stats[f"{key}_calls"] += 1
        self._stats[f"{key}_time"] += end - start

    def _stats_update_volume(self, key, amount):
        self._stats[f"{key}_volume"] += amount

    def _stats_get_volume(self, key):
        return self._stats.get(f"{key}_volume", 0)

    @property
    def stats(self):
        """
        return statistics like method call counters, overall time [ns], overall data volume, overall throughput.

        please note that the stats values only consider what is seen on the Store api:

        - there might be additional time spent by the caller, outside of Store, thus:

          - real time is longer.
          - real throughput is lower.
        - there are some overheads not accounted for, e.g. the volume only adds up the data size of load and store.
        - write buffering or cached reads might give a wrong impression.
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
            st[f"{key}_throughput"] = v / t
        return st

    def _get_levels(self, name):
        """get levels from configuration depending on namespace"""
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
        for level in self._get_levels(name):
            nested_name = nest(name, level, add_suffix=suffix)
            info = self.backend.info(nested_name)
            if info.exists:
                break
        return nested_name

    def info(self, name: str, *, deleted=False) -> ItemInfo:
        with self._stats_updater("info"):
            return self.backend.info(self.find(name, deleted=deleted))

    def load(self, name: str, *, size=None, offset=0, deleted=False) -> bytes:
        with self._stats_updater("load"):
            result = self.backend.load(self.find(name, deleted=deleted), size=size, offset=offset)
            self._stats_update_volume("load", len(result))
            return result

    def store(self, name: str, value: bytes) -> None:
        # note: using .find here will:
        # - overwrite an existing item (level stays same)
        # - write to the last level if no existing item is found.
        with self._stats_updater("store"):
            self.backend.store(self.find(name), value)
            self._stats_update_volume("store", len(value))

    def delete(self, name: str, *, deleted=False) -> None:
        """
        Really and immediately deletes an item.

        See also .move(name, delete=True) for "soft" deletion.
        """
        with self._stats_updater("delete"):
            self.backend.delete(self.find(name, deleted=deleted))

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
        elif undelete:
            # use case: keep name, undelete a previously soft "deleted" item
            nested_name = self.find(name, deleted=True)
            nested_new_name = nested_name.removesuffix(DEL_SUFFIX)
        elif change_level:
            # use case: keep name, changing to another nesting level
            suffix = DEL_SUFFIX if deleted else None
            nested_name = self.find(name, deleted=deleted)
            nested_new_name = nest(name, self._get_levels(name)[-1], add_suffix=suffix)
        else:
            # generic use (be careful!)
            if not new_name:
                raise ValueError("generic move needs new_name to be given.")
            nested_name = self.find(name, deleted=deleted)
            nested_new_name = self.find(new_name, deleted=deleted)
        with self._stats_updater("move"):
            self.backend.move(nested_name, nested_new_name)

    def list(self, name: str, deleted: bool = False) -> Iterator[ItemInfo]:
        """
        List all names in the namespace <name>.

        If deleted is True and soft deleted items are encountered, they are yielded
        as if they were not deleted. Otherwise, they are ignored.

        backend.list giving us sorted names implies store.list is also sorted, if all items are stored on same level.
        """
        # we need this wrapper due to the recursion - we only want to increment list_calls once:
        self._stats["list_calls"] += 1
        yield from self._list(name, deleted=deleted)

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
                elif not is_deleted:
                    yield info
