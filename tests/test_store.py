"""
Testing for high-level Store API.

For simplicity, a lot of tests do not use namespaces if they do not require more than one.
While this works for these tests, this is not recommended for production!
"""
import pytest

from . import key, list_store_names
from .test_backends import posixfs_backend, sftp_backend, sftp_is_available  # noqa

from borgstore.constants import ROOTNS
from borgstore.store import Store, ItemInfo


def test_basics(posixfs_backend):
    store = Store(backend=posixfs_backend, levels={ROOTNS: [2]})

    k0, v0 = key(0), b"value0"

    # roundtrip
    store.store(k0, v0)
    assert store.load(k0) == v0

    # check on higher level: store (automatic nesting)
    assert store.info(k0).exists
    assert not store.info(k0).directory
    assert store.info(k0).size == len(v0)
    # check on lower level: backend (no automatic nesting)
    assert store.backend.info("00").exists
    assert store.backend.info("00").directory
    assert store.backend.info("00/00").exists
    assert store.backend.info("00/00").directory
    assert store.backend.info("00/00/00000000").exists
    assert store.backend.info("00/00/00000000").size == len(v0)
    assert not store.backend.info("00/00/00000000").directory

    assert list(store.list(ROOTNS)) == [ItemInfo(name=k0, exists=True, size=len(v0), directory=False)]

    store.delete(k0)

    # check on higher level: store (automatic nesting)
    assert not store.info(k0).exists
    # check on lower level: backend (no automatic nesting)
    assert not store.backend.info("00/00/00000000").exists

    assert list(store.list(ROOTNS)) == []


@pytest.mark.parametrize(
    "levels,count",
    [
        ({ROOTNS: [0]}, 100),
        ({ROOTNS: [1]}, 1000),
        # ({ROOTNS: [2]}, 100000),  # takes rather long
    ],
)
def test_scalability_count(posixfs_backend, levels, count):
    store = Store(backend=posixfs_backend, levels=levels)
    keys = [key(i) for i in range(count)]
    for k in keys:
        store.store(k, b"")
    assert list_store_names(store, ROOTNS) == keys


@pytest.mark.skipif(not sftp_is_available, reason="SFTP is not available")
def test_scalability_big_values(sftp_backend):
    levels = {ROOTNS: [0]}
    count = 10
    value = b"x" * 2**20
    store = Store(backend=sftp_backend, levels=levels)
    keys = [key(i) for i in range(count)]
    for k in keys:
        store.store(k, value)
    for k in keys:
        assert store.load(k) == value
    assert list_store_names(store, ROOTNS) == keys


def test_file_url(tmp_path):
    from borgstore.backends.posixfs import PosixFS

    store = Store(url=f"file://{tmp_path}")
    assert isinstance(store.backend, PosixFS)
    assert store.backend.base_path == tmp_path


@pytest.mark.parametrize(
    "url,username,hostname,port,path",
    [
        ("sftp://username@hostname:2222/some/path", "username", "hostname", 2222, "/some/path"),
        ("sftp://username@hostname/some/path", "username", "hostname", 22, "/some/path"),
    ],
)
def test_sftp_url(url, username, hostname, port, path):
    from borgstore.backends.sftp import Sftp

    store = Store(url=url)
    assert isinstance(store.backend, Sftp)
    assert store.backend.username == username
    assert store.backend.hostname == hostname
    assert store.backend.port == port
    assert store.backend.base_path == path


def test_upgrade_levels(posixfs_backend):
    k0, v0 = key(0), b"value0"
    ii0 = ItemInfo(k0, True, len(v0), False)
    k1, v1 = key(1), b"value1"
    ii1 = ItemInfo(k1, True, len(v0), False)

    # start using the backend storage with nesting level 0:
    store = Store(backend=posixfs_backend, levels={ROOTNS: [0]})
    # store k0 on level 0:
    store.store(k0, v0)
    assert store.find(k0) == "" + k0  # found on level 0
    assert store.info(k0) == ii0
    assert list_store_names(store, ROOTNS) == [k0]

    # now upgrade to nesting level 1 (while keeping support for level 0), using the same backend storage:
    store = Store(backend=posixfs_backend, levels={ROOTNS: [0, 1]})
    # does k0 still work?
    assert store.find(k0) == "" + k0  # found on level 0
    assert store.info(k0) == ii0
    assert list_store_names(store, ROOTNS) == [k0]
    # store k1 on level 1:
    store.store(k1, v1)
    assert store.find(k1) == "00/" + k1  # found on level 1
    assert store.info(k1) == ii1
    assert list_store_names(store, ROOTNS) == [k0, k1]
    store.delete(k1)  # just to have it out of the way

    # check what happens when overwriting k0 (on level 0) with a new value:
    v0new = b"value0new"
    ii0new = ItemInfo(k0, True, 9, False)
    store.store(k0, v0new)
    assert store.find(k0) == "" + k0  # still found on level 0
    assert store.info(k0) == ii0new
    # k0 should show up only once as we wrote over the level 0 item:
    assert list_store_names(store, ROOTNS) == [k0]
    assert store.load(k0) == v0new


def test_downgrade_levels(posixfs_backend):
    k0, v0 = key(0), b"value0"
    ii0 = ItemInfo(k0, True, len(v0), False)
    k1, v1 = key(1), b"value1"
    ii1 = ItemInfo(k1, True, len(v0), False)

    # start using the backend storage with nesting level 1:
    store = Store(backend=posixfs_backend, levels={ROOTNS: [1]})
    # store k1 on level 1:
    store.store(k1, v1)
    assert store.find(k1) == "00/" + k1  # found on level 1
    assert store.info(k1) == ii1
    assert list_store_names(store, ROOTNS) == [k1]

    # now downgrade to nesting level 0 (while keeping support for level 1), using the same backend storage:
    store = Store(backend=posixfs_backend, levels={ROOTNS: [1, 0]})
    # does k1 still work?
    assert store.find(k1) == "00/" + k1  # found on level 1
    assert store.info(k1) == ii1
    assert list_store_names(store, ROOTNS) == [k1]
    # store k0 on level 0:
    store.store(k0, v0)
    assert store.find(k0) == "" + k0  # found on level 0
    assert store.info(k0) == ii0
    assert list_store_names(store, ROOTNS) == [k0, k1]
    store.delete(k0)  # just to have it out of the way

    # check what happens when overwriting k1 (on level 1) with a new value:
    v1new = b"value1new"
    ii1new = ItemInfo(k1, True, 9, False)
    store.store(k1, v1new)
    assert store.find(k1) == "00/" + k1  # still found on level 1
    assert store.info(k1) == ii1new
    # k1 should show up only once as we wrote over the level 1 item:
    assert list_store_names(store, ROOTNS) == [k1]
    assert store.load(k1) == v1new


def test_move_delete_undelete(posixfs_backend):
    k0, v0 = key(0), b"value0"
    k1, v1 = key(1), b"value1"
    store = Store(backend=posixfs_backend)
    store.store(k0, v0)
    store.store(k1, v1)
    # delete
    store.move(k0, delete=True)  # soft delete
    assert list_store_names(store, ROOTNS, deleted=False) == [k1]
    assert list_store_names(store, ROOTNS, deleted=True) == [k0, k1]
    # undelete
    store.move(k0, undelete=True)  # undelete previously soft deleted item
    assert list_store_names(store, ROOTNS, deleted=False) == [k0, k1]
    assert list_store_names(store, ROOTNS, deleted=True) == [k0, k1]


def test_move_change_level(posixfs_backend):
    k0, v0 = key(0), b"value0"
    store = Store(backend=posixfs_backend, levels={ROOTNS: [0]})
    store.store(k0, v0)  # store on level 0
    assert store.find(k0) == "" + k0  # now on level 0
    store = Store(backend=posixfs_backend, levels={ROOTNS: [0, 1]})
    store.move(k0, change_level=True)
    assert store.find(k0) == "00/" + k0  # now on level 1


def test_move_generic(posixfs_backend):
    # rename, stay in same namespace/directory
    k_curr, k_new, value = "ns/aaa", "ns/bbb", b"value"
    store = Store(backend=posixfs_backend)
    store.store(k_curr, value)
    store.move(k_curr, k_new)
    assert store.load(k_new) == value
    # move, change namespace/directory
    k_curr, k_new, value = "ns_curr/key", "ns_new/key", b"value"
    store = Store(backend=posixfs_backend)
    store.store(k_curr, value)
    store.move(k_curr, k_new)
    assert store.load(k_new) == value


def test_nesting_config(posixfs_backend):
    empty = b""
    levels_config = {
        ROOTNS: [0],
        "flat/": [0],
        "nested_one/": [1],
        "nested_two/": [2],
    }  # trailing slashes are important
    store = Store(backend=posixfs_backend, levels=levels_config)
    store.store("toplevel", empty)
    store.store("flat/something", empty)
    store.store("nested_one/0000", empty)
    store.store("nested_two/00000000", empty)
    assert store.find("toplevel") == "toplevel"
    assert store.find("flat/something") == "flat/something"
    assert store.find("nested_one/something") == "nested_one/so/something"
    assert store.find("nested_two/something") == "nested_two/so/me/something"
    # we do not have a levels_config entry for this, default is no nesting:
    assert store.find("no_config/something") == "no_config/something"
