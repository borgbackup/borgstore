"""
Generic testing for the misc. backend implementations.
"""
import pytest

from . import key, list_names

from borgstore.backends._base import ItemInfo
from borgstore.backends.posixfs import PosixFS
from borgstore.backends.sftp import Sftp
from borgstore.constants import ROOTNS


@pytest.fixture()
def posixfs_backend(tmp_path):
    be = PosixFS(tmp_path / "store")
    be.create()
    be.open()
    try:
        yield be
    finally:
        be.close()
        be.destroy()


def get_sftp_backend():
    # needs an authorized key loaded into the ssh agent. pytest works, tox doesn't:
    return Sftp(username="tw", hostname="localhost", path="/Users/tw/w/borgstore/temp-store")


def check_sftp_available():
    """in some test environments, get_sftp_backend() does not result in a working sftp backend"""
    try:
        be = get_sftp_backend()
        be.create()  # first sftp activity happens here
    except Exception:
        return False
    else:
        be.destroy()
        return True


sftp_is_available = check_sftp_available()


@pytest.fixture(scope="function")
def sftp_backend():
    be = get_sftp_backend()
    be.create()
    be.open()
    try:
        yield be
    finally:
        be.close()
        be.destroy()


def pytest_generate_tests(metafunc):
    # Generates tests for misc. storages
    if "tested_backends" in metafunc.fixturenames:
        tested_backends = ["posixfs_backend"]
        if sftp_is_available:
            tested_backends += ["sftp_backend"]
        metafunc.parametrize("tested_backends", tested_backends)


def get_backend_from_fixture(tested_backends, request):
    # returns the backend object from the fixture for tests that run on misc. backends
    return request.getfixturevalue(tested_backends)


def test_flat(tested_backends, request):
    backend = get_backend_from_fixture(tested_backends, request)

    k0, v0 = key(0), b"value0"
    k1, v1 = key(1), b"value1"
    k2 = key(2)
    k42 = key(42)

    assert sorted(backend.list(ROOTNS)) == []

    backend.store(k0, v0)
    i0 = backend.info(k0)
    assert i0.exists
    assert i0.size == len(v0)
    assert not i0.directory
    assert backend.load(k0) == v0
    assert list_names(backend, ROOTNS) == [k0]

    backend.store(k1, v1)
    assert backend.info(k1).exists
    assert backend.load(k1) == v1
    assert list_names(backend, ROOTNS) == sorted([k0, k1])

    backend.delete(k0)
    assert not backend.info(k0).exists
    assert list_names(backend, ROOTNS) == [k1]

    backend.move(k1, k2)
    assert not backend.info(k1).exists
    assert backend.info(k2).exists
    assert list_names(backend, ROOTNS) == [k2]

    backend.delete(k2)
    assert not backend.info(k2).exists
    assert list_names(backend, ROOTNS) == []

    assert not backend.info(k42).exists

    with pytest.raises(KeyError):
        backend.load(k42)

    with pytest.raises(KeyError):
        backend.delete(k42)


def test_namespaced(tested_backends, request):
    backend = get_backend_from_fixture(tested_backends, request)

    k0, v0, ns0 = key(0), b"value0", "data"
    k1, v1, ns1 = key(1), b"value1", "meta"
    k2 = key(2)
    k42, ns42 = key(42), "ns42"

    assert sorted(backend.list(ROOTNS)) == []

    backend.mkdir(ns0)
    backend.store(ns0 + "/" + k0, v0)
    assert backend.info(ns0 + "/" + k0).exists
    assert not backend.info(ns1 + "/" + k0).exists
    assert backend.load(ns0 + "/" + k0) == v0
    assert list_names(backend, ns0) == [k0]

    ins0 = backend.info(ns0)
    assert ins0.exists
    assert ins0.directory
    assert ins0.size == 0

    backend.mkdir(ns1)
    backend.store(ns1 + "/" + k1, v1)
    assert backend.info(ns1 + "/" + k1).exists
    assert not backend.info(ns0 + "/" + k1).exists
    assert backend.load(ns1 + "/" + k1) == v1
    assert list_names(backend, ns1) == [k1]

    backend.delete(ns0 + "/" + k0)
    assert not backend.info(ns0 + "/" + k0).exists
    assert list_names(backend, ns0) == []

    backend.move(ns1 + "/" + k1, ns1 + "/" + k2)
    assert not backend.info(ns1 + "/" + k1).exists
    assert backend.info(ns1 + "/" + k2).exists
    assert list_names(backend, ns1) == [k2]

    backend.delete(ns1 + "/" + k2)
    assert not backend.info(ns1 + "/" + k2).exists
    assert list_names(backend, ns1) == []

    assert list_names(backend, ROOTNS) == ["data", "meta"]

    assert not backend.info(ns0 + "/" + k42).exists

    with pytest.raises(KeyError):
        backend.load(ns0 + "/" + k42)

    with pytest.raises(KeyError):
        backend.delete(ns0 + "/" + k42)

    assert not backend.info(ns42 + "/" + k42).exists

    with pytest.raises(KeyError):
        backend.load(ns42 + "/" + k42)

    with pytest.raises(KeyError):
        backend.delete(ns42 + "/" + k42)

    backend.rmdir(ns0)
    backend.rmdir(ns1)
    assert list_names(backend, ROOTNS) == []


def test_invalid_name(tested_backends, request):
    backend = get_backend_from_fixture(tested_backends, request)

    with pytest.raises(ValueError):
        backend.info("/etc/passwd")  # absolute path is invalid

    with pytest.raises(ValueError):
        backend.info("../etc/passwd")  # ../ in path is invalid

    with pytest.raises(ValueError):
        backend.info("foo/../etc/passwd")  # ../ in path is invalid


def test_list(tested_backends, request):
    backend = get_backend_from_fixture(tested_backends, request)

    k0, v0 = key(0), b"value0"
    k1, v1 = key(1), b"value1"
    backend.store(k0, v0)
    backend.store(k1, v1)
    backend.mkdir("dir")
    items = list(backend.list(ROOTNS))
    assert len(items) == 3
    assert ItemInfo(name=k0, exists=True, size=len(v0), directory=False) in items
    assert ItemInfo(name=k1, exists=True, size=len(v1), directory=False) in items
    assert ItemInfo(name="dir", exists=True, size=0, directory=True) in items

    items = list(backend.list("dir"))
    assert items == []

    with pytest.raises(KeyError):
        list(backend.list("nonexistent"))


@pytest.mark.parametrize("exp", range(9))
def test_scalability_size(tested_backends, exp, request):
    size = 10**exp
    backend = get_backend_from_fixture(tested_backends, request)
    key, value = "key", bytes(size)
    backend.store("key", value)
    assert backend.load("key") == value
