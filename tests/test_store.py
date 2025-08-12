"""
Tests for the high-level Store API.
"""

import pytest

from . import key, list_store_names, list_store_names_sorted
from .test_backends import get_posixfs_test_backend  # noqa
from .test_backends import get_sftp_test_backend, sftp_is_available  # noqa
from .test_backends import get_rclone_test_backend, rclone_is_available  # noqa
from .test_backends import get_s3_test_backend, s3_is_available  # noqa

from borgstore.constants import ROOTNS
from borgstore.store import Store, ItemInfo

LEVELS_CONFIG = {"zero/": [0], "one/": [1], "two/": [2]}  # Layout used for most tests


@pytest.fixture()
def posixfs_store_created(tmp_path):
    store = Store(backend=get_posixfs_test_backend(tmp_path), levels=LEVELS_CONFIG)
    store.create()
    try:
        yield store
    finally:
        store.destroy()


@pytest.fixture()
def sftp_store_created():
    store = Store(backend=get_sftp_test_backend(), levels=LEVELS_CONFIG)
    store.create()
    try:
        yield store
    finally:
        store.destroy()


@pytest.fixture()
def rclone_store_created():
    store = Store(backend=get_rclone_test_backend(), levels=LEVELS_CONFIG)
    store.create()
    try:
        yield store
    finally:
        store.destroy()

@pytest.fixture()
def s3_store_created():
    store = Store(backend=get_s3_test_backend(), levels=LEVELS_CONFIG)
    store.create()
    try:
        yield store
    finally:
        store.destroy()


def test_basics(posixfs_store_created):
    ns = "two"
    k0 = key(0)
    v0 = b"value0"
    nsk0 = ns + "/" + k0
    with posixfs_store_created as store:
        # Round-trip
        store.store(nsk0, v0)
        assert store.load(nsk0) == v0

        # Check at a higher level: Store (automatic nesting)
        assert store.info(nsk0).exists
        assert not store.info(nsk0).directory
        assert store.info(nsk0).size == len(v0)
        # Check at a lower level: backend (no automatic nesting)
        assert store.backend.info("two/00").exists
        assert store.backend.info("two/00").directory
        assert store.backend.info("two/00/00").exists
        assert store.backend.info("two/00/00").directory
        assert store.backend.info("two/00/00/00000000").exists
        assert store.backend.info("two/00/00/00000000").size == len(v0)
        assert not store.backend.info("two/00/00/00000000").directory

        assert list(store.list(ns)) == [ItemInfo(name=k0, exists=True, size=len(v0), directory=False)]

        store.delete(nsk0)

        # Check at a higher level: Store (automatic nesting)
        assert not store.info(nsk0).exists
        # Check at a lower level: backend (no automatic nesting)
        assert not store.backend.info("two/00/00/00000000").exists

        assert list(store.list(ns)) == []


@pytest.mark.parametrize("namespace,count", [("zero", 100), ("one", 1000)])
def test_scalability_count(posixfs_store_created, namespace, count):
    with posixfs_store_created as store:
        keys = [key(i) for i in range(count)]
        for k in keys:
            store.store(namespace + "/" + k, b"")
        assert list_store_names(store, namespace) == keys


@pytest.mark.skipif(not sftp_is_available, reason="SFTP is not available")
def test_scalability_big_values_sftp(sftp_store_created):
    count = 10
    ns = "zero"
    value = b"x" * 2**20
    with sftp_store_created as store:
        keys = [key(i) for i in range(count)]
        for k in keys:
            store.store(ns + "/" + k, value)
        for k in keys:
            assert store.load(ns + "/" + k) == value
        assert list_store_names(store, ns) == keys


@pytest.mark.skipif(not rclone_is_available, reason="rclone is not available")
def test_scalability_big_values_rclone(rclone_store_created):
    count = 10
    ns = "zero"
    value = b"x" * 2**20
    with rclone_store_created as store:
        keys = [key(i) for i in range(count)]
        for k in keys:
            store.store(ns + "/" + k, value)
        for k in keys:
            assert store.load(ns + "/" + k) == value
        assert list_store_names(store, ns) == keys


@pytest.mark.skipif(not s3_is_available, reason="s3 is not available")
def test_scalability_big_values_s3(s3_store_created):
    count = 10
    ns = "zero"
    value = b"x" * 2**20
    with s3_store_created as store:
        keys = [key(i) for i in range(count)]
        for k in keys:
            store.store(ns + "/" + k, value)
        for k in keys:
            assert store.load(ns + "/" + k) == value
        assert list_store_names(store, ns) == keys


def test_upgrade_levels(posixfs_store_created):
    k0, v0 = key(0), b"value0"
    ii0 = ItemInfo(k0, True, len(v0), False)
    k1, v1 = key(1), b"value1"
    ii1 = ItemInfo(k1, True, len(v0), False)

    # Start using the backend storage with nesting level 0:
    posixfs_store_created.set_levels({ROOTNS: [0]}, create=True)
    with posixfs_store_created as store:
        # Store k0 on level 0:
        store.store(k0, v0)
        assert store.find(k0) == "" + k0  # found on level 0
        assert store.info(k0) == ii0
        assert list_store_names(store, ROOTNS) == [k0]

    # Now upgrade to nesting level 1 (while keeping support for level 0), using the same backend storage:
    posixfs_store_created.set_levels({ROOTNS: [0, 1]}, create=True)
    with posixfs_store_created as store:
        # Does k0 still work?
        assert store.find(k0) == "" + k0  # found on level 0
        assert store.info(k0) == ii0
        assert list_store_names(store, ROOTNS) == [k0]
        # Store k1 on level 1:
        store.store(k1, v1)
        assert store.find(k1) == "00/" + k1  # found on level 1
        assert store.info(k1) == ii1
        assert list_store_names_sorted(store, ROOTNS) == [k0, k1]
        store.delete(k1)  # just to have it out of the way

        # Check what happens when overwriting k0 (on level 0) with a new value:
        v0new = b"value0new"
        ii0new = ItemInfo(k0, True, 9, False)
        store.store(k0, v0new)
        assert store.find(k0) == "" + k0  # still found on level 0
        assert store.info(k0) == ii0new
        # k0 should show up only once, as we overwrote the level 0 item:
        assert list_store_names(store, ROOTNS) == [k0]
        assert store.load(k0) == v0new


def test_downgrade_levels(posixfs_store_created):
    k0, v0 = key(0), b"value0"
    ii0 = ItemInfo(k0, True, len(v0), False)
    k1, v1 = key(1), b"value1"
    ii1 = ItemInfo(k1, True, len(v0), False)

    # Start using the backend storage with nesting level 1:
    posixfs_store_created.set_levels({ROOTNS: [1]}, create=True)
    with posixfs_store_created as store:
        # Store k1 on level 1:
        store.store(k1, v1)
        assert store.find(k1) == "00/" + k1  # found on level 1
        assert store.info(k1) == ii1
        assert list_store_names(store, ROOTNS) == [k1]

    # Now downgrade to nesting level 0 (while keeping support for level 1), using the same backend storage:
    posixfs_store_created.set_levels({ROOTNS: [1, 0]}, create=True)
    with posixfs_store_created as store:
        # Does k1 still work?
        assert store.find(k1) == "00/" + k1  # found on level 1
        assert store.info(k1) == ii1
        assert list_store_names(store, ROOTNS) == [k1]
        # Store k0 on level 0:
        store.store(k0, v0)
        assert store.find(k0) == "" + k0  # found on level 0
        assert store.info(k0) == ii0
        assert list_store_names_sorted(store, ROOTNS) == [k0, k1]
        store.delete(k0)  # just to have it out of the way

        # Check what happens when overwriting k1 (on level 1) with a new value:
        v1new = b"value1new"
        ii1new = ItemInfo(k1, True, 9, False)
        store.store(k1, v1new)
        assert store.find(k1) == "00/" + k1  # still found on level 1
        assert store.info(k1) == ii1new
        # k1 should show up only once, as we overwrote the level 1 item:
        assert list_store_names(store, ROOTNS) == [k1]
        assert store.load(k1) == v1new


def test_move_delete_undelete(posixfs_store_created):
    ns = "zero"
    k0, v0 = key(0), b"value0"
    nsk0 = ns + "/" + k0
    k1, v1 = key(1), b"value1"
    nsk1 = ns + "/" + k1
    with posixfs_store_created as store:
        store.store(nsk0, v0)
        store.store(nsk1, v1)
        # Delete
        store.move(nsk0, delete=True)  # soft-delete
        assert list_store_names(store, ns, deleted=False) == [k1]
        assert list_store_names(store, ns, deleted=True) == [k0]
        # Undelete
        store.move(nsk0, undelete=True)  # undelete a previously soft-deleted item
        assert list_store_names(store, ns, deleted=False) == [k0, k1]
        assert list_store_names(store, ns, deleted=True) == []


def test_move_change_level(posixfs_store_created):
    k0, v0 = key(0), b"value0"
    posixfs_store_created.set_levels({ROOTNS: [0]}, create=True)
    with posixfs_store_created as store:
        store.store(k0, v0)  # Store on level 0
        assert store.find(k0) == "" + k0  # Now on level 0
    posixfs_store_created.set_levels({ROOTNS: [0, 1]}, create=True)
    with posixfs_store_created as store:
        store.move(k0, change_level=True)
        assert store.find(k0) == "00/" + k0  # Now on level 1


def test_move_generic(posixfs_store_created):
    # Rename; stay in the same namespace/directory
    k_curr, k_new, value = "zero/aaa", "zero/bbb", b"value"
    with posixfs_store_created as store:
        store.store(k_curr, value)
        store.move(k_curr, k_new)
        assert store.load(k_new) == value
    # Move; change namespace/directory
    k_curr, k_new, value = "one/00000000", "two/00000000", b"value"
    with posixfs_store_created as store:
        store.store(k_curr, value)
        store.move(k_curr, k_new)
        assert store.load(k_new) == value


def test_nesting_config(posixfs_store_created):
    empty = b""
    with posixfs_store_created as store:
        store.store("zero/something", empty)
        store.store("one/1234", empty)
        store.store("two/12345678", empty)
        assert store.find("zero/something") == "zero/something"
        assert store.find("one/1234") == "one/12/1234"
        assert store.find("two/12345678") == "two/12/34/12345678"


def test_load_partial(posixfs_store_created):
    key = "zero/key"
    with posixfs_store_created as store:
        store.store(key, b"0123456789")
        assert store.load(key) == b"0123456789"
        assert store.load(key, size=3) == b"012"
        assert store.load(key, offset=5) == b"56789"
        assert store.load(key, offset=4, size=4) == b"4567"


def test_list_is_sorted(posixfs_store_created):
    # The flat list we get from backend.list is sorted.
    # If all items are on the same level, this implies that Store.list is also sorted,
    # although it performs no sorting of its own.
    empty = b""
    unsorted_keys = "0012", "0000", "9999", "9988", "5566", "6655", "3322", "3300"
    sorted_keys = sorted(unsorted_keys)
    with posixfs_store_created as store:
        for key in unsorted_keys:
            store.store(f"zero/{key}", empty)
        assert list_store_names(store, "zero") == sorted_keys
        for key in unsorted_keys:
            store.store(f"one/{key}", empty)
        assert list_store_names(store, "one") == sorted_keys
        for key in unsorted_keys:
            store.store(f"two/{key}", empty)
        assert list_store_names(store, "two") == sorted_keys


def test_stats(posixfs_store_created):
    with posixfs_store_created as store:
        ns, key, value = "zero", "zero/key", b""
        assert store._stats == {}
        # Calls
        store.store(key, value)
        assert store._stats["store_calls"] == 1
        store.store(key, value)
        assert store._stats["store_calls"] == 2
        store.load(key)
        assert store._stats["load_calls"] == 1
        assert store._stats["store_calls"] == 2
        list(store.list(ns))
        assert store._stats["list_calls"] == 1
        # Timings (in ns; thus > 0 in any case)
        assert store._stats["list_time"] > 0
        assert store._stats["load_time"] > 0
        assert store._stats["store_time"] > 0
        # Volume
        assert store._stats["load_volume"] == 0
        assert store._stats["store_volume"] == 0
        value = bytes(100)
        store.store(key, value)
        assert store._stats["store_volume"] == 100
        store.store(key, value)
        assert store._stats["store_volume"] == 200
        store.load(key)
        assert store._stats["load_volume"] == 100
        store.load(key)
        assert store._stats["load_volume"] == 200
