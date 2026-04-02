"""
Filesystem-based backend implementation - uses files in directories below a base path.
"""

import hashlib
import os
import re
import sys
import time
from urllib.parse import unquote
from pathlib import Path
import shutil
import stat
import tempfile
import types

is_win32 = sys.platform == "win32"

fcntl: types.ModuleType | None
try:
    import fcntl
except ImportError:
    fcntl = None  # not available on Windows

from ._base import BackendBase, ItemInfo, validate_name
from .errors import BackendError, BackendAlreadyExists, BackendDoesNotExist, BackendMustNotBeOpen, BackendMustBeOpen
from .errors import ObjectNotFound, PermissionDenied, QuotaExceeded
from ..constants import TMP_SUFFIX, QUOTA_STORE_NAME, QUOTA_PERSIST_DELTA, QUOTA_PERSIST_INTERVAL


def get_file_backend(url, permissions=None, quota=None):
    # file:///absolute/path
    # notes:
    # - we only support **local** fs **absolute** paths.
    # - there is no such thing as a "relative path" local fs file: URL
    # - the general URL syntax is proto://host/path
    # - // introduces the host part. it is empty here, meaning localhost / local fs.
    # - the third slash is NOT optional, it is the start of an absolute path as well
    #   as the separator between the host and the path part.
    # - the caller is responsible to give an absolute path.
    # - Windows: see: https://en.wikipedia.org/wiki/File_URI_scheme
    windows_file_regex = r"""
        file://  # only empty host part is supported.
        /  # 3rd slash is separator ONLY, not part of the path.
        (?P<drive_and_path>([a-zA-Z]:/.*))  # path must be an absolute path.
    """
    file_regex = r"""
        file://  # only empty host part is supported.
        (?P<path>(/.*))  # path must be an absolute path. 3rd slash is separator AND part of the path.
    """
    # the path or drive_and_path could be URL-quoted and thus must be URL-unquoted
    if sys.platform in ("win32", "msys", "cygwin"):
        m = re.match(windows_file_regex, url, re.VERBOSE)
        if m:
            return PosixFS(path=unquote(m["drive_and_path"]), permissions=permissions, quota=quota)
    m = re.match(file_regex, url, re.VERBOSE)
    if m:
        return PosixFS(path=unquote(m["path"]), permissions=permissions, quota=quota)


class PosixFS(BackendBase):
    # PosixFS implementation supports precreate = True as well as = False.
    precreate_dirs: bool = False

    def __init__(self, path, *, do_fsync=False, permissions=None, quota=None):
        self.base_path = Path(path)
        if not self.base_path.is_absolute():
            raise BackendError(f"path must be an absolute path: {path}")
        self.opened = False
        self.do_fsync = do_fsync  # False = 26x faster, see #10
        self.permissions = permissions or {}  # name [str] -> granted_permissions [str]
        self.quota = quota  # maximum allowed storage size in bytes, None means unlimited
        self._quota_use = 0  # current tracked storage usage in bytes
        self._quota_use_persisted = 0  # last persisted value
        self._quota_last_persist_time = 0.0  # monotonic time of last persist

    def _check_permission(self, name, required_permissions):
        """
        Check in the self.permissions mapping if one of the
        required_permissions is granted for the given name or its parents.

        Permission characters:
        - l: allow listing object names ("namespace/directory listing")
        - r: allow reading objects (contents)
        - w: allow writing NEW objects (must not already exist)
        - W: allow writing objects (also overwrite existing objects)
        - D: allow deleting objects

        Move requires "D" (src) and "wW" (dst).
        Moves are used by the Store for soft-deletion/undeletion, level changes and generic renames.

        If permissions are granted for a directory like "foo", they also apply to objects
        below that directory, like "foo/bar".
        """
        assert set(required_permissions).issubset("lrwWD")

        if not self.permissions:  # If no permissions dict is provided, allow all operations.
            return

        # Check permissions, starting from full name (full path) going up to the root.
        path_parts = name.split("/")
        for i in range(len(path_parts), -1, -1):  # i: LEN .. 0
            path = "/".join(path_parts[:i])  # path: full path .. root
            if path in self.permissions:
                granted_permissions = self.permissions[path]
                # Check if any of the required permissions is present.
                if set(required_permissions) & set(granted_permissions):
                    return  # Permission granted
                # If path was found in permissions but didn't have required permission, we stop here
                # (more specific longer-path entry takes precedence over shorter-path entry).
                break

        # If we get here, none of the required permissions was found
        raise PermissionDenied(f"One of permissions '{required_permissions}' required for '{name}'")

    def create(self):
        if self.opened:
            raise BackendMustNotBeOpen()
        self._check_permission("", "wW")
        # we accept an already existing empty directory and we also optionally create
        # any missing parent dirs. the latter is important for repository hosters that
        # only offer limited access to their storage (e.g. only via borg/borgstore).
        # also, it is simpler than requiring users to create parent dirs separately.
        self.base_path.mkdir(exist_ok=True, parents=True)
        # avoid that users create a mess by using non-empty directories:
        contents = list(self.base_path.iterdir())
        if contents:
            raise BackendAlreadyExists(f"posixfs storage base path is not empty: {self.base_path}")

    def destroy(self):
        if self.opened:
            raise BackendMustNotBeOpen()
        self._check_permission("", "D")
        if not self.base_path.exists():
            raise BackendDoesNotExist(f"posixfs storage base path does not exist: {self.base_path}")

        def onexc(func, path, exc):
            # for rmtree, this is called if it can't remove a file or directory.
            # usually, this is because of missing permissions.
            if path != os.fspath(self.base_path):
                raise exc
            # do not raise if we can't remove the base path directory.
            # .create accepts an already existing base path, thus
            # .destroy may leave an existing base path behind.

        def onerror(func, path, excinfo):
            onexc(func, path, excinfo[1])

        kw = {"onexc": onexc} if sys.version_info >= (3, 12) else {"onerror": onerror}
        shutil.rmtree(os.fspath(self.base_path), **kw)

    def open(self):
        if self.opened:
            raise BackendMustNotBeOpen()
        if not self.base_path.is_dir():
            raise BackendDoesNotExist(
                f"posixfs storage base path does not exist or is not a directory: {self.base_path}"
            )
        if self.quota is not None:
            self._quota_persist(0)
        else:
            self._quota_delete()
        self.opened = True

    def close(self):
        if not self.opened:
            raise BackendMustBeOpen()
        if self.quota is not None:
            self._quota_update(0, force=True)
        self.opened = False

    def _validate_join(self, name):
        validate_name(name)
        return self.base_path / name

    def mkdir(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        path = self._validate_join(name)
        # spamming a store with lots of random empty dirs == DoS, thus require w.
        self._check_permission(name, "w")
        path.mkdir(parents=True, exist_ok=True)

    def rmdir(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        path = self._validate_join(name)
        # path.rmdir only removes empty directories, thus no data can be lost.
        # thus, a granted "w" is already good enough, "D" is also ok.
        self._check_permission(name, "wD")
        try:
            path.rmdir()
        except FileNotFoundError:
            raise ObjectNotFound(name) from None

    def info(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        path = self._validate_join(name)
        # we do not read object content, so a granted "l" is enough, "r" is also ok.
        self._check_permission(name, "lr")
        try:
            st = path.stat()
        except FileNotFoundError:
            return ItemInfo(name=path.name, exists=False, directory=False, size=0)
        else:
            is_dir = stat.S_ISDIR(st.st_mode)
            return ItemInfo(name=path.name, exists=True, directory=is_dir, size=st.st_size)

    def load(self, name, *, size=None, offset=0):
        if not self.opened:
            raise BackendMustBeOpen()
        path = self._validate_join(name)
        self._check_permission(name, "r")
        try:
            with path.open("rb") as f:
                if offset != 0:
                    f.seek(offset, os.SEEK_SET if offset >= 0 else os.SEEK_END)
                return f.read(-1 if size is None else size)
        except FileNotFoundError:
            raise ObjectNotFound(name) from None

    def _write_to_tempfile(self, path, value, suffix=TMP_SUFFIX, do_fsync=False):
        with tempfile.NamedTemporaryFile(suffix=suffix, dir=path, delete=False) as f:
            f.write(value)
            if do_fsync:
                f.flush()
                os.fsync(f.fileno())
            tmp_path = Path(f.name)
        return tmp_path

    def store(self, name, value):
        if not self.opened:
            raise BackendMustBeOpen()
        path = self._validate_join(name)
        overwrite = path.exists()
        self._check_permission(name, "W" if overwrite else "wW")
        if self.quota is not None:
            old_size = path.stat().st_size if overwrite else 0
            new_size = len(value)
            delta = new_size - old_size
            if self._quota_use + delta > self.quota:
                raise QuotaExceeded(f"Quota exceeded: {self._quota_use + delta} > {self.quota}")
        tmp_dir = path.parent
        # write to a differently named temp file in same directory first,
        # so the store never sees partially written data.
        try:
            # try to do it quickly, not doing the mkdir. fs ops might be slow, esp. on network fs (latency).
            # this will frequently succeed, because the dir is already there.
            tmp_path = self._write_to_tempfile(tmp_dir, value, do_fsync=self.do_fsync)
        except FileNotFoundError:
            # retry, create potentially missing dirs first. this covers these cases:
            # - either the dirs were not precreated
            # - a previously existing directory was "lost" in the filesystem
            tmp_dir.mkdir(parents=True, exist_ok=True)
            tmp_path = self._write_to_tempfile(tmp_dir, value, do_fsync=self.do_fsync)
        # all written and synced to disk, rename it to the final name:
        try:
            tmp_path.replace(path)
        except OSError:
            tmp_path.unlink()
            raise
        if self.quota is not None:
            self._quota_update(delta)

    def delete(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        path = self._validate_join(name)
        self._check_permission(name, "D")
        try:
            if self.quota is not None:
                size = path.stat().st_size
            path.unlink()
        except FileNotFoundError:
            raise ObjectNotFound(name) from None
        if self.quota is not None:
            self._quota_update(-size)

    def move(self, curr_name, new_name):
        def _rename_to_new_name():
            curr_path.rename(new_path)

        if not self.opened:
            raise BackendMustBeOpen()
        curr_path = self._validate_join(curr_name)
        new_path = self._validate_join(new_name)
        # random moves could do a lot of harm in the store:
        # not finding an object anymore is similar to having it deleted.
        # also, the source object vanishes under its original name, thus we want D for the source.
        # as the move might replace the destination, we want W or wW for the destination.
        # move is also used for soft-deletion by the Store, that also hints to using D for the source.
        self._check_permission(curr_name, "D")
        self._check_permission(new_name, "W" if new_path.exists() else "wW")
        try:
            # try to do it quickly, not doing the mkdir. fs ops might be slow, esp. on network fs (latency).
            # this will frequently succeed, because the dir is already there.
            _rename_to_new_name()
        except FileNotFoundError:
            # retry, create potentially missing dirs first. this covers these cases:
            # - either the dirs were not precreated
            # - a previously existing directory was "lost" in the filesystem
            new_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                _rename_to_new_name()
            except FileNotFoundError:
                raise ObjectNotFound(curr_name) from None

    def defrag(self, sources, *, target=None, algorithm=None, namespace=None, levels=0) -> str:
        if not self.opened:
            raise BackendMustBeOpen()
        # check all permissions before doing anything
        prefix = namespace.rstrip("/") + "/" if namespace else ""
        # if target is not given, an item named like content-hash is created in same namespace.
        check_target = target if target else prefix + "01234567"
        self._check_permission(check_target, "W")
        names = [prefix + source[0] for source in sources]
        for name in names:
            self._check_permission(name, "r")
        return super().defrag(sources, target=target, algorithm=algorithm, namespace=namespace, levels=levels)

    def hash(self, name: str, algorithm: str = "sha256") -> str:
        if not self.opened:
            raise BackendMustBeOpen()
        path = self._validate_join(name)
        self._check_permission(name, "r")
        try:
            h = hashlib.new(algorithm)
        except ValueError:
            raise ValueError(f"Unsupported hash algorithm: {algorithm}") from None
        try:
            with path.open("rb") as f:
                while True:
                    data = f.read(1024 * 1024)
                    if not data:
                        break
                    h.update(data)
        except FileNotFoundError:
            raise ObjectNotFound(name) from None
        return h.hexdigest()

    def list(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        path = self._validate_join(name)
        self._check_permission(name, "l")
        try:
            paths = sorted(path.iterdir())
        except FileNotFoundError:
            raise ObjectNotFound(name) from None
        else:
            for p in paths:
                try:
                    validate_name(p.name)
                except ValueError:
                    pass  # that file is likely not from us or is still uploading
                else:
                    try:
                        st = p.stat()
                    except FileNotFoundError:
                        pass
                    else:
                        is_dir = stat.S_ISDIR(st.st_mode)
                        yield ItemInfo(name=p.name, exists=True, size=st.st_size, directory=is_dir)

    def _quota_path(self):
        return self.base_path / QUOTA_STORE_NAME

    def _quota_scan(self, path, skips):
        """Scan the filesystem to determine actual storage usage."""
        total = 0
        with os.scandir(path) as it:
            for entry in it:
                if entry.is_file(follow_symlinks=False):
                    if os.path.abspath(entry.path) not in skips:
                        total += entry.stat(follow_symlinks=False).st_size
                elif entry.is_dir(follow_symlinks=False):
                    total += self._quota_scan(entry.path, skips)
        return total

    def _quota_persist(self, delta):
        """Persist quota usage to the on-disk quota file.

        To support concurrent sessions, this method applies the given *delta*
        to the current on-disk value under an exclusive file lock.  This way,
        updates from other sessions are preserved.

        If the quota file does not exist or contains an invalid value, a
        filesystem scan is performed to determine the actual usage.

        The quota file itself is used as the lock file (opened and locked
        with flock) so no separate lock file is needed.
        """
        quota_path = self._quota_path()
        try:
            fd = os.open(str(quota_path), os.O_RDONLY)
        except FileNotFoundError:
            # quota file missing, scan filesystem to determine usage
            skips = {os.path.abspath(quota_path)}
            quota_use = self._quota_scan(self.base_path, skips)
            quota_path.write_text(str(quota_use))
            self._quota_use_persisted = quota_use
            self._quota_use = quota_use
            self._quota_last_persist_time = time.monotonic()
            return
        try:
            if fcntl is not None:
                fcntl.flock(fd, fcntl.LOCK_EX)
            # read current on-disk value (may have been updated by another session)
            try:
                on_disk = int(os.read(fd, 100))
            except ValueError:
                # invalid content, scan filesystem to determine usage
                skips = {os.path.abspath(quota_path)}
                on_disk = self._quota_scan(self.base_path, skips)
                delta = 0  # scan already gives the true value
            if is_win32:
                # Close the file before replacing to avoid AccessDenied on Windows.
                os.close(fd)
                fd = -1
            new_value = max(on_disk + delta, 0)
            quota_content = str(new_value).encode()
            tmp_path = self._write_to_tempfile(quota_path.parent, quota_content, do_fsync=True)
            try:
                tmp_path.replace(quota_path)  # atomic update
            except OSError:
                tmp_path.unlink()
                raise
            self._quota_use_persisted = new_value
            self._quota_use = new_value  # re-sync with on-disk truth
            self._quota_last_persist_time = time.monotonic()
        finally:
            if fcntl is not None:
                fcntl.flock(fd, fcntl.LOCK_UN)
            if fd >= 0:
                os.close(fd)

    def _quota_update(self, delta, force=False):
        """Update quota usage by delta and persist if the change is significant or enough time has elapsed."""
        self._quota_use += delta
        persist_delta = self._quota_use - self._quota_use_persisted
        elapsed = time.monotonic() - self._quota_last_persist_time
        if force or abs(persist_delta) >= QUOTA_PERSIST_DELTA or elapsed >= QUOTA_PERSIST_INTERVAL:
            self._quota_persist(persist_delta)

    def _quota_delete(self):
        """Delete the quota file if it exists."""
        try:
            self._quota_path().unlink()
        except FileNotFoundError:
            pass
