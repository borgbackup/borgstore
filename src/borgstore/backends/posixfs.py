"""
Filesystem based backend implementation - uses files in directories below a base path.
"""

import os
import re
from pathlib import Path
import shutil
import stat
import tempfile

from ._base import BackendBase, ItemInfo, validate_name
from .errors import BackendAlreadyExists, BackendDoesNotExist, BackendMustNotBeOpen, BackendMustBeOpen, ObjectNotFound
from ..constants import TMP_SUFFIX


def get_file_backend(url):
    # file:///absolute/path
    # or
    # file://relative/path (temporary hack, might change in future)
    file_regex = r"""
        file://
        (?P<path>(.*))
    """
    m = re.match(file_regex, url, re.VERBOSE)
    if m:
        return PosixFS(path=m["path"])


class PosixFS(BackendBase):
    def __init__(self, path, *, do_fsync=False):
        self.base_path = Path(path).absolute()
        self.opened = False
        self.do_fsync = do_fsync  # False = 26x faster, see #10

    def create(self):
        if self.opened:
            raise BackendMustNotBeOpen()
        try:
            self.base_path.mkdir()
        except FileExistsError:
            raise BackendAlreadyExists(f"posixfs storage base path already exists: {self.base_path}")

    def destroy(self):
        if self.opened:
            raise BackendMustNotBeOpen()
        try:
            shutil.rmtree(os.fspath(self.base_path))
        except FileNotFoundError:
            raise BackendDoesNotExist(f"posixfs storage base path does not exist: {self.base_path}")

    def open(self):
        if self.opened:
            raise BackendMustNotBeOpen()
        if not self.base_path.is_dir():
            raise BackendDoesNotExist(
                f"posixfs storage base path does not exist or is not a directory: {self.base_path}"
            )
        self.opened = True

    def close(self):
        if not self.opened:
            raise BackendMustBeOpen()
        self.opened = False

    def _validate_join(self, name):
        validate_name(name)
        return self.base_path / name

    def mkdir(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        path = self._validate_join(name)
        path.mkdir(parents=True, exist_ok=True)

    def rmdir(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        path = self._validate_join(name)
        try:
            path.rmdir()
        except FileNotFoundError:
            raise ObjectNotFound(name) from None

    def info(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        path = self._validate_join(name)
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
        try:
            with path.open("rb") as f:
                if offset > 0:
                    f.seek(offset)
                return f.read(-1 if size is None else size)
        except FileNotFoundError:
            raise ObjectNotFound(name) from None

    def store(self, name, value):
        if not self.opened:
            raise BackendMustBeOpen()
        path = self._validate_join(name)
        tmp_dir = path.parent
        tmp_dir.mkdir(parents=True, exist_ok=True)
        # write to a differently named temp file in same directory first,
        # so the store never sees partially written data.
        with tempfile.NamedTemporaryFile(suffix=TMP_SUFFIX, dir=tmp_dir, delete=False) as f:
            f.write(value)
            if self.do_fsync:
                f.flush()
                os.fsync(f.fileno())
            tmp_path = Path(f.name)
        # all written and synced to disk, rename it to the final name:
        try:
            tmp_path.replace(path)
        except OSError:
            tmp_path.unlink()
            raise

    def delete(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        path = self._validate_join(name)
        try:
            path.unlink()
        except FileNotFoundError:
            raise ObjectNotFound(name) from None

    def move(self, curr_name, new_name):
        if not self.opened:
            raise BackendMustBeOpen()
        curr_path = self._validate_join(curr_name)
        new_path = self._validate_join(new_name)
        try:
            new_path.parent.mkdir(parents=True, exist_ok=True)
            curr_path.replace(new_path)
        except FileNotFoundError:
            raise ObjectNotFound(curr_name) from None

    def list(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        path = self._validate_join(name)
        try:
            paths = sorted(path.iterdir())
        except FileNotFoundError:
            raise ObjectNotFound(name) from None
        else:
            for p in paths:
                if not p.name.endswith(TMP_SUFFIX):
                    try:
                        st = p.stat()
                    except FileNotFoundError:
                        pass
                    else:
                        is_dir = stat.S_ISDIR(st.st_mode)
                        yield ItemInfo(name=p.name, exists=True, size=st.st_size, directory=is_dir)
