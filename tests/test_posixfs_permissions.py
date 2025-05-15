# PosixFS implements a special .permissions instance attribute that has a map
# from names (paths) [str] to granted permissions [str].
# This can be used to test software which uses borgstore to check whether it
# behaves well in scenarios with different permissions (like read-only, object
# deletion disallowed or all permissions granted).

import pytest

from borgstore.backends.posixfs import PosixFS
from borgstore.backends.errors import PermissionDenied


DATA1, DATA2 = b"data1", b"data2"


def test_full_permissions(tmp_path):
    # all permissions granted, recursively.
    fs = PosixFS(path=tmp_path, permissions={"": "lrwWD"})
    # w
    fs.create()
    fs.open()
    fs.mkdir("dir")
    # w
    fs.store("dir/file", DATA1)
    # W
    fs.store("dir/file", DATA2)
    # r
    assert fs.load("dir/file") == DATA2
    # l
    list(fs.list(""))  # should not raise exception
    list(fs.list("dir"))  # should not raise exception
    # r and w
    fs.move("dir/file", "dir/moved_file")
    # D
    fs.delete("dir/moved_file")
    fs.close()


def test_readonly_permissions(tmp_path):
    fs = PosixFS(path=tmp_path, permissions={"": "w"})  # permissions needed for setup
    fs.create()
    fs.open()
    fs.mkdir("dir")
    fs.store("dir/file", DATA1)
    # read-only permissions granted, recursively.
    fs.permissions = {"": "lr"}
    # w denied
    with pytest.raises(PermissionDenied):
        fs.store("dir/file2", DATA2)
    # W denied
    with pytest.raises(PermissionDenied):
        fs.store("dir/file", DATA2)
    # r
    assert fs.load("dir/file") == DATA1
    # l
    list(fs.list(""))  # should not raise exception
    list(fs.list("dir"))  # should not raise exception
    # D denied
    with pytest.raises(PermissionDenied):
        fs.delete("dir/file")
    # r granted, but w denied
    with pytest.raises(PermissionDenied):
        fs.move("dir/file", "dir/moved_file")
    fs.close()
    # D denied
    with pytest.raises(PermissionDenied):
        fs.destroy()


def test_nodelete_permissions(tmp_path):
    fs = PosixFS(path=tmp_path, permissions={"": "w"})  # permissions needed for setup
    fs.create()
    fs.open()
    fs.mkdir("dir")
    fs.store("dir/file", DATA1)
    # no-delete no-overwrite permissions granted, recursively.
    fs.permissions = {"": "lrw"}  # no W, no D
    # w
    fs.store("dir/file2", DATA2)
    # W denied
    with pytest.raises(PermissionDenied):
        fs.store("dir/file", DATA2)
    # r
    assert fs.load("dir/file") == DATA1
    # l
    list(fs.list(""))  # should not raise exception
    list(fs.list("dir"))  # should not raise exception
    # D denied
    with pytest.raises(PermissionDenied):
        fs.delete("dir/file")
    # D (src) and w (dst), D denied.
    with pytest.raises(PermissionDenied):
        fs.move("dir/file", "dir/moved_file")
    fs.close()
    # D denied
    with pytest.raises(PermissionDenied):
        fs.destroy()


def test_permission_lookup(tmp_path):
    fs = PosixFS(path=tmp_path, permissions={"": "w"})  # permissions needed for setup
    fs.create()
    fs.open()
    fs.mkdir("dir")
    fs.store("dir/file", DATA1)
    # no-delete no-overwrite permissions granted, recursively.
    fs.permissions = {
        "": "l",  # we only allow list at top-level
        "dir": "lrw",  # adding new stuff in dir is allowed
        "dir/file": "r",  # but for file, only reading is allowed
    }
    # checks permissions for "not-allowed" (unknown) and "" (known: l only). mkdir wants w -> denied.
    with pytest.raises(PermissionDenied):
        fs.mkdir("not-allowed")
    # checks permissions for "dir/file2" (unknown) and "dir" (known: lrw). store wants w -> granted.
    fs.store("dir/file2", DATA2)
    # checks permissions for "dir/file" (known: r). store wants W -> denied.
    with pytest.raises(PermissionDenied):
        fs.store("dir/file", DATA2)
    fs.close()
    # checks permissions for "" (known: l). destroy wants D -> denied.
    with pytest.raises(PermissionDenied):
        fs.destroy()
