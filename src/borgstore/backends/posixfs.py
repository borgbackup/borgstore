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
from ..constants import TMP_SUFFIX


def get_file_backend(url):
    # file:///var/backups/borgstore/first
    file_regex = r"""
        file://
        (?P<path>(/.*))
    """
    m = re.match(file_regex, url, re.VERBOSE)
    if m:
        return PosixFS(path=m["path"])


class PosixFS(BackendBase):
    def __init__(self, path):
        self.base_path = Path(path)
        self.opened = False

    def create(self):
        if self.opened:
            raise self.MustNotBeOpen()
        self.base_path.mkdir()

    def destroy(self):
        if self.opened:
            raise self.MustNotBeOpen()
        shutil.rmtree(os.fspath(self.base_path))

    def open(self):
        if self.opened:
            raise self.MustNotBeOpen()
        if not self.base_path.is_dir():
            raise TypeError(f"storage base path does not exist or is not a directory: {self.base_path}")
        self.opened = True

    def close(self):
        if not self.opened:
            raise self.MustBeOpen()
        self.opened = False

    def _validate_join(self, name):
        validate_name(name)
        return self.base_path / name

    def mkdir(self, name):
        path = self._validate_join(name)
        path.mkdir(parents=True, exist_ok=True)

    def rmdir(self, name):
        path = self._validate_join(name)
        try:
            path.rmdir()
        except FileNotFoundError:
            raise KeyError(name) from None

    def info(self, name):
        path = self._validate_join(name)
        try:
            st = path.stat()
        except FileNotFoundError:
            return ItemInfo(name=path.name, exists=False, directory=False, size=0)
        else:
            is_dir = stat.S_ISDIR(st.st_mode)
            size = 0 if is_dir else st.st_size
            return ItemInfo(name=path.name, exists=True, directory=is_dir, size=size)

    def load(self, name, *, size=None, offset=0):
        path = self._validate_join(name)
        try:
            with path.open("rb") as f:
                if offset > 0:
                    f.seek(offset)
                return f.read(-1 if size is None else size)
        except FileNotFoundError:
            raise KeyError(name) from None

    def store(self, name, value):
        path = self._validate_join(name)
        tmp_dir = path.parent
        tmp_dir.mkdir(parents=True, exist_ok=True)
        # write to a differently named temp file in same directory first,
        # so the store never sees partially written data.
        with tempfile.NamedTemporaryFile(suffix=TMP_SUFFIX, dir=tmp_dir, delete=False) as f:
            f.write(value)
            f.flush()
            fd = f.fileno()
            os.fsync(fd)
            tmp_path = Path(f.name)
        # all written and synced to disk, rename it to the final name:
        try:
            tmp_path.replace(path)
        except OSError:
            tmp_path.unlink()
            raise

    def delete(self, name):
        path = self._validate_join(name)
        try:
            path.unlink()
        except FileNotFoundError:
            raise KeyError(name) from None

    def move(self, curr_name, new_name):
        curr_path = self._validate_join(curr_name)
        new_path = self._validate_join(new_name)
        try:
            new_path.parent.mkdir(parents=True, exist_ok=True)
            curr_path.replace(new_path)
        except FileNotFoundError:
            raise KeyError(curr_name) from None

    def list(self, name):
        path = self._validate_join(name)
        try:
            for p in path.iterdir():
                try:
                    st = p.stat()
                except FileNotFoundError:
                    pass
                else:
                    if not p.name.endswith(TMP_SUFFIX):
                        is_dir = stat.S_ISDIR(st.st_mode)
                        size = 0 if is_dir else st.st_size
                        yield ItemInfo(name=p.name, exists=True, size=size, directory=is_dir)
        except FileNotFoundError:
            raise KeyError(name) from None
