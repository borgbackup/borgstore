"""
Multi-Store Key/Value Implementation.

Distributed: MStore can store into multiple stores (e.g. different directories on different disks, on diff. servers)
             with different sizes.
Redundant: The same mechanism also implements simple redundancy (like storing same item N times).

Similar to a hashtable, we use 256 buckets within the MStore and create a map mapping the bucket number to the Store(s)
it resides on. When storing an item, the key part of the name (namespace/key) is assumed to be a hex hash value and
the first 2 hex digits determine which bucket the data goes into (and thus: which Store(s) it is stored into).

Examples:
MStore gets a list of stores and a list of related bucket counts. Bucket numbers are calculated modulo 256, so if
the total bucket count is more than 256 (like 512, 768, ...), stuff will get stored multiple times (usually into
different stores).
MStore([store0], [256]) - simplest configuration: store everything into store0
MStore([st0, st1], [192, 64]) - JBOD-like: store 3/4 into st0 (bucket 0..191), 1/4 into st1 (bucket 192..255)
MStore([st0, st1], [256, 256]) - Mirror: store each item into st0 **and** into st1 (both have buckets 0..255)
MStore([st0, st1, st2], [256, 256, 256]) - store each item into st0, st1 **and** st2
"""

from collections import defaultdict
from typing import Iterator, Optional

from .utils.nesting import split_key
from .store import Store, ItemInfo, ObjectNotFound


def create_bucket_map(buckets: list[int]) -> dict[int, list[int]]:
    """
    use a list of bucket counts (of the stores) and create a lookup dictionary:
    bucket (0..255) -> list of store indexes that store this bucket
    """
    total = sum(buckets)
    if total < 256:
        raise ValueError("each of the 256 possible values must have at least one corresponding bucket")
    if total % 256 != 0:
        raise ValueError("all 256 values should be covered equally with buckets")
    map = defaultdict(list)
    base = 0
    for store_index, bucket_count in enumerate(buckets):
        for offset in range(bucket_count):
            bucket = (base + offset) % 256
            map[bucket].append(store_index)
        base += bucket_count
    return map


def lookup_stores(map: dict, bucket: int) -> list[int]:
    """lookup the store index(es) for a specific bucket"""
    if not isinstance(bucket, int):
        raise TypeError("bucket must be an integer")
    if bucket < 0 or bucket > 255:
        raise ValueError("bucket must be between 0 and 255")
    return map[bucket]


class MStore:
    def __init__(self, stores: list[Store], buckets: list[int], kinds: Optional[dict] = None):
        if not len(stores):
            raise ValueError("stores list must not be empty")
        if len(stores) != len(buckets):
            raise ValueError("stores list and buckets count list must have same length")
        self.stores = stores
        self.all_stores = list(range(len(self.stores)))
        self.map = create_bucket_map(buckets)
        # kinds = prefix -> kind, kind can be "hex-hash", "generic".
        kinds = kinds if kinds else {}
        # we accept kinds as a dict, but we rather want a list of (prefix, kind) tuples, longest prefix first:
        self.kinds = [entry for entry in sorted(kinds.items(), key=lambda item: len(item[0]), reverse=True)]

    def create(self) -> None:
        for store in self.stores:
            store.create()

    def destroy(self) -> None:
        for store in self.stores:
            store.destroy()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def open(self) -> None:
        for store in self.stores:
            store.open()

    def close(self) -> None:
        for store in self.stores:
            store.close()

    def _get_kind(self, name):
        """get kind of store from configuration depending on namespace"""
        for prefix, kind in self.kinds:
            if name.startswith(prefix):
                return kind
        return "generic"  # "generic" is the default, if no prefix matched

    def _find_stores(self, name: str, mode: str = "r") -> list[int]:
        kind = self._get_kind(name)
        if kind == "hex-hash":
            key = split_key(name)[1]  # we do not care for the namespace part here
            key_binary = bytes.fromhex(key)  # and assume key is a good hash, represented as a hex str
            bucket = key_binary[0]  # use first 8bits of key to determine bucket (int)
            w_stores = self.map[bucket]  # list of store indexes (for writing)
            if mode not in ["r", "w", "d", "m"]:
                raise ValueError("mode must be either 'r', 'w', 'd' or 'm'.")
            if mode == "w":
                # for writing just return the stores currently configured
                return w_stores
            else:  # mode == "r" or "d" or "m"
                # for reading, return the stores currently configured *first*,
                # but also add all other stores after these, so items can be found
                # there while we redistribute them.
                # for deleting, guess we also want to try deleting an item from all stores.
                # for moving, guess we want to try to move an item in all stores.
                fallback_r_stores = [idx for idx in self.all_stores if idx not in w_stores]
            return w_stores + fallback_r_stores
        elif kind == "generic":
            # for generic storage, we store to ALL stores.
            # usually this is important and small stuff, like configs, keys, ...
            return self.all_stores
        else:
            raise NotImplementedError(f"kind '{kind}' is not implemented.")

    def info(self, name: str, *, deleted=False) -> ItemInfo:
        for store_idx in self._find_stores(name, mode="r"):
            store = self.stores[store_idx]
            try:
                return store.info(name, deleted=deleted)
            except ObjectNotFound:
                pass  # TODO: we expected the key to be there, but it was not. fix that by storing it there.
        else:
            raise ObjectNotFound(name)  # didn't find it in any store

    def load(self, name: str, *, size=None, offset=0, deleted=False) -> bytes:
        for store_idx in self._find_stores(name, mode="r"):
            store = self.stores[store_idx]
            try:
                return store.load(name, size=size, offset=offset, deleted=deleted)
            except ObjectNotFound:
                pass  # TODO: we expected the key to be there, but it was not. fix that by storing it there.
        else:
            raise ObjectNotFound(name)  # didn't find it in any store

    def store(self, name: str, value: bytes) -> None:
        for store_idx in self._find_stores(name, mode="w"):
            store = self.stores[store_idx]
            store.store(name, value)

    def delete(self, name: str, *, deleted=False) -> None:
        for store_idx in self._find_stores(name, mode="d"):
            store = self.stores[store_idx]
            try:
                store.delete(name, deleted=deleted)
            except ObjectNotFound:
                pass  # ignore it if it is already gone

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
        for store_idx in self._find_stores(name, mode="m"):
            store = self.stores[store_idx]
            try:
                if delete:
                    # use case: keep name, but soft "delete" the item
                    store.move(name, delete=True)
                elif undelete:
                    # use case: keep name, undelete a previously soft "deleted" item
                    store.move(name, undelete=True)
                elif change_level:
                    # use case: keep name, changing to another nesting level
                    store.move(name, change_level=True, deleted=deleted)
                else:
                    # generic use (be careful!)
                    if not new_name:
                        raise ValueError("generic move needs new_name to be given.")
                    store.move(name, new_name, deleted=deleted)
            except ObjectNotFound:
                pass  # ignore it, if it is not present in this store

    def list(self, name: str, deleted: bool = False) -> Iterator[ItemInfo]:
        seen = set()
        for store in self.stores:
            for item_info in store.list(name, deleted=deleted):
                if item_info.name not in seen:
                    yield item_info
                    seen.add(item_info.name)
