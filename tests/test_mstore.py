"""
Testing for high-level MStore API.
"""
import pytest

from . import key, lkey, list_store_names, list_store_names_sorted

from borgstore.backends.errors import ObjectNotFound
from borgstore.store import Store
from borgstore.mstore import create_bucket_map, lookup_stores, MStore


@pytest.mark.parametrize("buckets", [[], [0], [42], [300], [256, 23], [23, 256]])
def test_bucket_map_invalid(buckets):
    with pytest.raises(ValueError):
        create_bucket_map(buckets)  # does not cover 256 buckets exactly N times


@pytest.mark.parametrize(
    "buckets, n_stores",
    [
        ([256], 1),  # single store having all buckets ("single disk")
        ([128, 128], 1),  # 2 stores each having half of the buckets ("raid0")
        ([256, 256], 2),  # 2 stores each having all the buckets ("raid1")
        ([128, 128, 128, 128], 2),  # 4 stores each having half of the buckets ("raid10")
        ([256, 128, 128], 2),  # one big store mirroring 2 smaller ones
        ([200, 56], 1),  # store 0 is bigger than store 1 ("jbod")
        ([256, 256, 256], 3),  # 3 stores each having all buckets ("3-disk mirror")
    ],
)
def test_bucket_map_valid(buckets, n_stores):
    # n_stores means an item is stored in n stores (1 = standard, 2+ = with redundancy)
    map = create_bucket_map(buckets)
    for bucket in range(256):
        assert bucket in map  # we want to map ALL the 256 buckets
        stores = map[bucket]
        assert len(stores) == n_stores  # each bucket shall exist in N stores
        assert len(set(stores)) == n_stores  # each bucket shall exist in N *different* stores


@pytest.mark.parametrize(
    "buckets,key,store",
    [
        ([256], 0, [0]),
        ([256], 255, [0]),
        ([128, 128], 0, [0]),
        ([128, 128], 127, [0]),
        ([128, 128], 128, [1]),
        ([128, 128], 255, [1]),
        ([256, 256], 0, [0, 1]),
        ([256, 256], 127, [0, 1]),
        ([256, 256], 128, [0, 1]),
        ([256, 256], 255, [0, 1]),
    ],
)
def test_lookup_bucket(buckets, key, store):
    map = create_bucket_map(buckets)
    assert lookup_stores(map, key) == store


@pytest.fixture()
def mstore_jbod_created(tmp_path):
    stores = [Store(url=f"file://{tmp_path}0"), Store(url=f"file://{tmp_path}1")]
    mstore = MStore(stores=stores, buckets=[192, 64], kinds={"": "hex-hash"})
    mstore.create()
    try:
        yield mstore
    finally:
        mstore.destroy()


@pytest.fixture()
def mstore_mirror_created(tmp_path):
    stores = [Store(url=f"file://{tmp_path}0"), Store(url=f"file://{tmp_path}1")]
    mstore = MStore(stores=stores, buckets=[256, 256], kinds={"": "hex-hash"})
    mstore.create()
    try:
        yield mstore
    finally:
        mstore.destroy()


def fill_storage(store: MStore, count: int, *, start: int = 0) -> None:
    for i in range(start, start + count, 1):
        k, v = lkey(i), str(i).encode()
        store.store(k, v)


def read_storage(store: MStore, count: int, *, start: int = 0) -> None:
    # can we still read all data?
    for i in range(start, start + count, 1):
        k, v = lkey(i), str(i).encode()
        assert store.load(k) == v


def test_list(mstore_mirror_created):
    with mstore_mirror_created as mstore:
        fill_storage(mstore, 1024)
        # there must be no duplication of keys from the mirror mstore
        assert list_store_names(mstore, "") == sorted([lkey(i) for i in range(1024)])


def test_list(mstore_jbod_created):
    with mstore_jbod_created as mstore:
        fill_storage(mstore, 1024)
        # check if we get all expected keys from the jbod mstore
        assert list_store_names(mstore, "") == sorted([lkey(i) for i in range(1024)])


def test_load_store_list_distribution(mstore_jbod_created):
    with mstore_jbod_created as mstore:
        fill_storage(mstore, 1024)
        # check if all data is readable and as expected:
        for i in range(1024):
            k, v = lkey(i), str(i).encode()
            assert mstore.load(k) == v
        # check if data ended up in the stores according to the ratio configured in mstore_jbod (192 : 64)
        keys_mstore = list_store_names(mstore, "")
        keys_store0 = list_store_names(mstore.stores[0], "")
        keys_store1 = list_store_names(mstore.stores[1], "")
        assert len(keys_mstore) == len(set(keys_mstore)) == 1024
        assert len(keys_store0) == len(set(keys_store0)) == 768
        assert len(keys_store1) == len(set(keys_store1)) == 256


def test_load_store_list_redundancy(mstore_mirror_created):
    with mstore_mirror_created as mstore:
        fill_storage(mstore, 1024)
        # delete stuff from store 0:
        for i in 0, 23, 42, 1001:
            mstore.stores[0].delete(lkey(i))
        # check if it is really gone:
        for i in 0, 23, 42, 1001:
            with pytest.raises(ObjectNotFound):
                mstore.stores[0].load(lkey(i))
        # delete other stuff from store 1:
        for i in 123, 456, 789:
            mstore.stores[1].delete(lkey(i))
        # check if it is really gone:
        for i in 123, 456, 789:
            with pytest.raises(ObjectNotFound):
                mstore.stores[1].load(lkey(i))
        # check if we can still read everything from the mirror:
        for i in range(1024):
            k, v = lkey(i), str(i).encode()
            assert mstore.load(k) == v
        # also check if list still works ok:
        assert list_store_names_sorted(mstore, "") == sorted([lkey(i) for i in range(1024)])
        # now delete some values also from the other side of the mirror:
        for i in 0, 23, 42, 1001:
            mstore.stores[1].delete(lkey(i))
        for i in 123, 456, 789:
            mstore.stores[0].delete(lkey(i))
        # now the mirror is expected to be partially corrupted at these places:
        for i in 0, 23, 42, 1001, 123, 456, 789:
            with pytest.raises(ObjectNotFound):
                mstore.load(lkey(i))
        # list is expected to miss some elements:
        assert list_store_names(mstore, "") == sorted(
            [lkey(i) for i in range(1024) if i not in [0, 23, 42, 1001, 123, 456, 789]]
        )


def test_move_delete_undelete(mstore_mirror_created):
    k0, v0 = key(0), b"value0"
    k1, v1 = key(1), b"value1"
    with mstore_mirror_created as mstore:
        mstore.store(k0, v0)
        mstore.store(k1, v1)
        # delete
        mstore.move(k0, delete=True)  # soft delete
        assert list_store_names(mstore, "", deleted=False) == [k1]
        assert list_store_names(mstore, "", deleted=True) == [k0, k1]
        # undelete
        mstore.move(k0, undelete=True)  # undelete previously soft deleted item
        assert list_store_names(mstore, "", deleted=False) == [k0, k1]
        assert list_store_names(mstore, "", deleted=True) == [k0, k1]


def test_namespaces(mstore_jbod_created):
    with mstore_jbod_created as mstore:
        mstore.kinds = [("config/", "generic"), ("data/", "hex-hash")]
        mstore.store("config/main", b"some config")
        mstore.store("data/0000", b"value_00")
        mstore.store("data/bf00", b"value_bf")
        mstore.store("data/c000", b"value_c0")
        mstore.store("data/ff00", b"value_ff")
        # now let's check where stuff ended up being stored.
        st0, st1 = mstore.stores
        # hex-hash kind of data should be spread into buckets according to its hash:
        assert st0.load("data/0000") == b"value_00"
        assert st0.load("data/bf00") == b"value_bf"
        with pytest.raises(ObjectNotFound):
            st0.load("data/c000")
        with pytest.raises(ObjectNotFound):
            st0.load("data/ff00")
        with pytest.raises(ObjectNotFound):
            st1.load("data/0000")
        with pytest.raises(ObjectNotFound):
            st1.load("data/bf00")
        assert st1.load("data/c000") == b"value_c0"
        assert st1.load("data/ff00") == b"value_ff"
        # generic kind config should be mirrored to all stores:
        assert st0.load("config/main") == b"some config"
        assert st1.load("config/main") == b"some config"


def test_reduce_prepare(tmp_path):
    # assume we want to stop using a store, then:
    # - we don't want to write new data to it
    # - we want to be able to read all data from the mstore at all times
    #
    # test setup: we have 3 stores with data distributed over them:
    entries = 1024
    stores = [Store(url=f"file://{tmp_path}0"), Store(url=f"file://{tmp_path}1"), Store(url=f"file://{tmp_path}2")]
    mstore = MStore(stores=stores, buckets=[128, 64, 64], kinds={"": "hex-hash"})
    mstore.create()
    with mstore:
        fill_storage(mstore, entries)
        read_storage(mstore, entries)
        assert len(list_store_names(mstore.stores[0], "")) == 512
        assert len(list_store_names(mstore.stores[1], "")) == 256
        assert len(list_store_names(mstore.stores[2], "")) == 256
    # test: still have the 3 stores available, but bucket count 0 in store 2 means no new data will go into it:
    stores = [Store(url=f"file://{tmp_path}0"), Store(url=f"file://{tmp_path}1"), Store(url=f"file://{tmp_path}2")]
    mstore = MStore(stores=stores, buckets=[128, 128, 0], kinds={"": "hex-hash"})
    with mstore:
        read_storage(mstore, entries)
        # store new stuff into the mstore:
        fill_storage(mstore, entries, start=entries)
        read_storage(mstore, entries * 2)
        assert len(list_store_names(mstore.stores[0], "")) == 512 + 512
        assert len(list_store_names(mstore.stores[1], "")) == 256 + 512
        assert len(list_store_names(mstore.stores[2], "")) == 256  # no new data was written to store 2
    mstore.destroy()
