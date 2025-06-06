"""
Generic testing for the misc. backend implementations.
"""

import os
from pathlib import Path

import pytest
import tempfile

from . import key, list_names

from borgstore.backends._base import ItemInfo
from borgstore.backends.errors import (
    BackendAlreadyExists,
    BackendDoesNotExist,
    BackendMustBeOpen,
    BackendMustNotBeOpen,
    ObjectNotFound,
)
from borgstore.backends.posixfs import PosixFS, get_file_backend
from borgstore.backends.sftp import Sftp, get_sftp_backend
from borgstore.backends.rclone import Rclone, get_rclone_backend
from borgstore.backends.s3 import S3, get_s3_backend
from borgstore.constants import ROOTNS, TMP_SUFFIX


def get_posixfs_test_backend(tmp_path):
    be = PosixFS(tmp_path / "store")
    return be


@pytest.fixture()
def posixfs_backend_created(tmp_path):
    be = get_posixfs_test_backend(tmp_path)
    be.create()
    try:
        yield be
    finally:
        be.destroy()


def get_sftp_test_backend():
    # export BORGSTORE_TEST_SFTP_URL="sftp://user@host:port/borgstore/temp-store"
    # please note that the path is relative, usually to the user's home directory on the server.
    # giving an absolute path: "sftp://user@host:port//home/user/borgstore/temp-store"
    # needs an authorized key loaded into the ssh agent. pytest works, tox doesn't.
    url = os.environ.get("BORGSTORE_TEST_SFTP_URL")
    if url:
        be = get_sftp_backend(url)
        return be


def check_sftp_available():
    """in some test environments, get_sftp_backend() does not result in a working sftp backend"""
    try:
        be = get_sftp_test_backend()
        be.create()  # first sftp activity happens here
    except Exception:
        return False  # use "raise" here for debugging sftp store issues
    else:
        be.destroy()
        return True


def get_rclone_test_backend():
    # To use a specific RCLONE backend
    # export BORGSTORE_TEST_RCLONE_URL="rclone:remote:path"
    # otherwise this will run an rclone backend in a temporary directory on local disk
    url = os.environ.get("BORGSTORE_TEST_RCLONE_URL")
    if not url:
        tempdir = tempfile.mkdtemp()
        # remove the temporary directory since we need to start without it
        os.rmdir(tempdir)
        url = f"rclone:{tempdir}"
    return get_rclone_backend(url)


def check_rclone_available():
    """in some test environments, get_rclone_backend() does not result in a working rclone backend"""
    try:
        be = get_rclone_test_backend()
        be.create()  # first rclone activity happens here
    except Exception as e:
        print(f"Rclone backend create failed {repr(e)}")
        return False  # use "raise" here for debugging rclone store issues
    else:
        be.destroy()
        return True

def get_s3_test_backend():
    # export BORGSTORE_TEST_S3_URL="s3:[profile|(access_key_id:access_key_secret)@][schema://hostname[:port]]/bucket/path"
    # export BORGSTORE_TEST_S3_URL="s3:/test/path"
    # export BORGSTORE_TEST_S3_URL="s3:test@http://172.28.52.116:9000/test/path"
    # export BORGSTORE_TEST_S3_URL="s3:test:testtest@http://172.28.52.116:9000/test/path"
    # export BORGSTORE_TEST_S3_URL="b2:test:testtest@https://s3.us-east-005.backblazeb2.com/test/path"
    url = os.environ.get("BORGSTORE_TEST_S3_URL")
    if not url:
        return None
    return get_s3_backend(url)


def check_s3_available():
    """in some test environments, get_s3_backend() does not result in a working s3 backend"""
    try:
        be = get_s3_test_backend()
        be.create()  # first s3 activity happens here
    except Exception as e:
        print(f"S3 backend create failed {repr(e)}")
        return False  # use "raise" here for debugging s3 store issues
    else:
        be.destroy()
        return True

sftp_is_available = check_sftp_available()
rclone_is_available = check_rclone_available()
s3_is_available = check_s3_available()


@pytest.fixture(scope="function")
def sftp_backend_created():
    be = get_sftp_test_backend()
    be.create()
    try:
        yield be
    finally:
        be.destroy()


@pytest.fixture(scope="function")
def rclone_backend_created():
    be = get_rclone_test_backend()
    be.create()
    try:
        yield be
    finally:
        be.destroy()


@pytest.fixture(scope="function")
def s3_backend_created():
    be = get_s3_test_backend()
    be.create()
    try:
        yield be
    finally:
        be.destroy()


def pytest_generate_tests(metafunc):
    # Generates tests for misc. storages
    if "tested_backends" in metafunc.fixturenames:
        tested_backends = ["posixfs_backend_created"]
        if sftp_is_available:
            tested_backends += ["sftp_backend_created"]
        if rclone_is_available:
            tested_backends += ["rclone_backend_created"]
        if s3_is_available:
            tested_backends += ["s3_backend_created"]
        metafunc.parametrize("tested_backends", tested_backends)


def get_backend_from_fixture(tested_backends, request):
    # returns the backend object from the fixture for tests that run on misc. backends
    return request.getfixturevalue(tested_backends)


@pytest.mark.parametrize(
    "url,path", [("file:///absolute/path", "/absolute/path")]  # first 2 slashes are to introduce host (empty here)
)
def test_file_url(url, path):
    backend = get_file_backend(url)
    assert isinstance(backend, PosixFS)
    assert backend.base_path == Path(path).absolute()


@pytest.mark.parametrize(
    "url",
    [
        # file://hostname/path is the generic pattern for file urls.
        # posixfs does not support non-local fs file urls (with a non-empty hostname part).
        # also, there is no such thing as a relative path local fs file url, #23:
        # - "relative" would be the hostname here, not a part of a relative path,
        #   but we do not support non-empty hostnames (== remote fs) in posixfs.
        # - "path" would be a network fs share name here, not a part of a relative path,
        #   this is also not supported in posixfs.
        "file://relative/path",  # invalid "relative path" URL
        "file://hostname/share",  # unsupported remote fs URL
    ],
)
def test_invalid_or_remote_file_url(url):
    backend = get_file_backend(url)
    # the url is not recognized by the posixfs backend.
    # this behaviour keeps the option open that we could have another backend that
    # can deal with non-local fs file urls.
    assert backend is None


@pytest.mark.skipif(not sftp_is_available, reason="SFTP is not available")
@pytest.mark.parametrize(
    "url,username,hostname,port,path",
    [
        ("sftp://username@hostname:2222/rel/path", "username", "hostname", 2222, "rel/path"),
        ("sftp://username@hostname/rel/path", "username", "hostname", 0, "rel/path"),
        ("sftp://hostname/rel/path", None, "hostname", 0, "rel/path"),
        ("sftp://username@hostname:2222//abs/path", "username", "hostname", 2222, "/abs/path"),
        ("sftp://username@hostname//abs/path", "username", "hostname", 0, "/abs/path"),
        ("sftp://hostname//abs/path", None, "hostname", 0, "/abs/path"),
    ],
)
def test_sftp_url(url, username, hostname, port, path):
    backend = get_sftp_backend(url)
    assert isinstance(backend, Sftp)
    assert backend.username == username
    assert backend.hostname == hostname
    assert backend.port == port  # note: 0 means "not given" (and will usually mean 22 in the end)
    assert backend.base_path == path


def test_flat(tested_backends, request):
    with get_backend_from_fixture(tested_backends, request) as backend:
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

        with pytest.raises(ObjectNotFound):
            backend.load(k42)

        with pytest.raises(ObjectNotFound):
            backend.delete(k42)


def test_namespaced(tested_backends, request):
    with get_backend_from_fixture(tested_backends, request) as backend:
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

        with pytest.raises(ObjectNotFound):
            backend.load(ns0 + "/" + k42)

        with pytest.raises(ObjectNotFound):
            backend.delete(ns0 + "/" + k42)

        assert not backend.info(ns42 + "/" + k42).exists

        with pytest.raises(ObjectNotFound):
            backend.load(ns42 + "/" + k42)

        with pytest.raises(ObjectNotFound):
            backend.delete(ns42 + "/" + k42)

        backend.rmdir(ns0)
        backend.rmdir(ns1)
        assert list_names(backend, ROOTNS) == []


def test_invalid_name(tested_backends, request):
    with get_backend_from_fixture(tested_backends, request) as backend:
        with pytest.raises(ValueError):
            backend.info("/etc/passwd")  # absolute path is invalid

        with pytest.raises(ValueError):
            backend.info("../etc/passwd")  # ../ in path is invalid

        with pytest.raises(ValueError):
            backend.info("foo/../etc/passwd")  # ../ in path is invalid


def test_list(tested_backends, request):
    with get_backend_from_fixture(tested_backends, request) as backend:
        k0, v0 = key(0), b"value0"
        k1, v1 = key(1), b"value1"
        backend.store(k0, v0)
        backend.store(k1, v1)
        backend.mkdir("dir")
        items = list(backend.list(ROOTNS))
        assert len(items) == 3
        assert ItemInfo(name=k0, exists=True, size=len(v0), directory=False) in items
        assert ItemInfo(name=k1, exists=True, size=len(v1), directory=False) in items
        # for "dir", we do not know what size the backend has returned.
        # that is rather OS / fs / backend specific.
        matching_items = [item for item in items if item.name == "dir"]
        assert len(matching_items) == 1
        dir_item = matching_items[0]
        assert dir_item.exists
        assert dir_item.directory

        items = list(backend.list("dir"))
        assert items == []

        with pytest.raises(ObjectNotFound):
            list(backend.list("nonexistent"))


def test_list_temporary_item(tested_backends, request):
    with get_backend_from_fixture(tested_backends, request) as backend:
        # usually, one must never use a key with TMP_SUFFIX, but we do it here
        # for the sake of creating an item with such a name (somehow like if a
        # temporary item was accidentally left in the backend storage).
        backend.store("file-while-uploading" + TMP_SUFFIX, b"value")
        assert list(backend.list(ROOTNS)) == []  # .list must not yield tmp files


@pytest.mark.parametrize("exp", range(9))
def test_scalability_size(tested_backends, exp, request):
    with get_backend_from_fixture(tested_backends, request) as backend:
        size = 10**exp
        key, value = "key", bytes(size)
        backend.store("key", value)
        assert backend.load("key") == value


def test_load_partial(tested_backends, request):
    with get_backend_from_fixture(tested_backends, request) as backend:
        backend.store("key", b"0123456789")
        assert backend.load("key") == b"0123456789"
        assert backend.load("key", size=3) == b"012"
        assert backend.load("key", offset=5) == b"56789"
        assert backend.load("key", offset=4, size=4) == b"4567"


def test_already_exists(tested_backends, request):
    backend = get_backend_from_fixture(tested_backends, request)
    with backend as _backend:
        _backend.store("key", b"value")  # make the backend "not empty"
    # the backend must reject (re-)creation if there is already something at that place:
    with pytest.raises(BackendAlreadyExists):
        backend.create()


def test_does_not_exist(tested_backends, request):
    backend = get_backend_from_fixture(tested_backends, request)
    # the backend is already created, but we do not want this here:
    backend.destroy()
    # now the backend does not exist anymore, trying to destroy it again errors:
    with pytest.raises(BackendDoesNotExist):
        backend.destroy()
    # create the backend again, so the context manager can happily destroy it:
    backend.create()


def test_must_be_open(tested_backends, request):
    backend = get_backend_from_fixture(tested_backends, request)
    with pytest.raises(BackendMustBeOpen):
        list(backend.list("dir"))
    with pytest.raises(BackendMustBeOpen):
        backend.mkdir("dir")
    with pytest.raises(BackendMustBeOpen):
        backend.rmdir("dir")
    with pytest.raises(BackendMustBeOpen):
        backend.store("key", b"value")
    with pytest.raises(BackendMustBeOpen):
        backend.load("key")
    with pytest.raises(BackendMustBeOpen):
        backend.info("key")
    with pytest.raises(BackendMustBeOpen):
        backend.move("key", "otherkey")
    with pytest.raises(BackendMustBeOpen):
        backend.close()


def test_must_not_be_open(tested_backends, request):
    backend = get_backend_from_fixture(tested_backends, request)
    backend.open()
    with pytest.raises(BackendMustNotBeOpen):
        backend.open()
    with pytest.raises(BackendMustNotBeOpen):
        backend.create()
    with pytest.raises(BackendMustNotBeOpen):
        backend.destroy()
    backend.close()  # needed for test teardown to succeed


def test_missing_nesting_dir_store(tested_backends, request):
    with get_backend_from_fixture(tested_backends, request) as backend:
        # for the unit tests to be fast, sftp and posixfs backends are created with
        # .precreate_dirs = False, so we do not have precreated nesting dirs.
        #
        # test so the code does not expect pre-created dirs, .store should do mkdir and write and succeed:
        assert not backend.precreate_dirs
        backend.store("namespace1/nest1/key1", b"value1")
        # test so the code expects pre-created dirs (but we do not have them!) and
        # initially does not do the mkdir. then it retries, does mkdir and write and succeeds:
        backend.precreate_dirs = True
        backend.store("namespace2/nest2/key2", b"value2")


def test_missing_nesting_dir_move(tested_backends, request):
    with get_backend_from_fixture(tested_backends, request) as backend:
        # similar as previous test for .store, but this tests .move method.
        # test so the code does not expect pre-created dirs, .move should do mkdir and move and succeed:
        assert not backend.precreate_dirs
        backend.store("namespace1/nest1/key1", b"value1")
        backend.move("namespace1/nest1/key1", "namespace1a/nest1a/key1a")
        # test so the code expects pre-created dirs (but we do not have them!) and
        # initially does not do the mkdir. then it retries, does mkdir and move and succeeds:
        backend.precreate_dirs = True
        backend.store("namespace2/nest2/key2", b"value2")
        backend.move("namespace2/nest2/key2", "namespace2a/nest2a/key2a")


def test_posixfs_missing_parent_dirs(tmp_path):
    be = PosixFS(tmp_path / "missing_parent_dir1" / "missing_parent_dir2" / "store")
    be.create()  # this should work, auto-creating the missing parent dir(s)
    # try to use the store to make sure it works
    be.open()
    try:
        be.store("key", b"value")
    finally:
        be.close()
