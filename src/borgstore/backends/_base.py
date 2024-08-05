"""
Base class and type definitions for all backend implementations in this package.

Docs that are not backend-specific are also found here.
"""

from abc import ABC, abstractmethod
from collections import namedtuple
from typing import Iterator

from ..constants import MAX_NAME_LENGTH

ItemInfo = namedtuple("ItemInfo", "name exists size directory")


def validate_name(name):
    """validate a backend key / name"""
    if not isinstance(name, str):
        raise TypeError(f"name must be str, but got: {type(name)}")
    # name must not be too long
    if len(name) > MAX_NAME_LENGTH:
        raise ValueError(f"name is too long (max: {MAX_NAME_LENGTH}): {name}")
    # avoid encoding issues
    try:
        name.encode("ascii")
    except UnicodeEncodeError:
        raise ValueError(f"name must encode to plain ascii, but failed with: {name}")
    # security: name must be relative - can be foo or foo/bar/baz, but must never be /foo or ../foo
    if name.startswith("/") or name.endswith("/") or ".." in name:
        raise ValueError(f"name must be relative and not contain '..': {name}")
    # names used here always have '/' as separator, never '\' -
    # this is to avoid confusion in case this is ported to e.g. Windows.
    # also: no blanks - simplifies usage via CLI / shell.
    if "\\" in name or " " in name:
        raise ValueError(f"name must not contain backslashes or blanks: {name}")
    # name must be lowercase - this is to avoid troubles in case this is ported to a non-case-sensitive backend.
    # also, guess we want to avoid that a key "config" would address a different item than a key "CONFIG" or
    # a key "1234CAFE5678BABE" would address a different item than a key "1234cafe5678babe".
    if name != name.lower():
        raise ValueError(f"name must be lowercase, but got: {name}")


class BackendBase(ABC):
    @abstractmethod
    def create(self):
        """create (initialize) a backend storage"""

    @abstractmethod
    def destroy(self):
        """completely remove the backend storage (and its contents)"""

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    @abstractmethod
    def open(self):
        """open (start using) a backend storage"""

    @abstractmethod
    def close(self):
        """close (stop using) a backend storage"""

    @abstractmethod
    def mkdir(self, name: str) -> None:
        """create directory/namespace <name>"""

    @abstractmethod
    def rmdir(self, name: str) -> None:
        """remove directory/namespace <name>"""

    @abstractmethod
    def info(self, name) -> ItemInfo:
        """return information about <name>"""

    @abstractmethod
    def load(self, name: str, *, size=None, offset=0) -> bytes:
        """load value from <name>"""

    @abstractmethod
    def store(self, name: str, value: bytes) -> None:
        """store <value> into <name>"""

    @abstractmethod
    def delete(self, name: str) -> None:
        """delete <name>"""

    @abstractmethod
    def move(self, curr_name: str, new_name: str) -> None:
        """rename curr_name to new_name (overwrite target)"""

    @abstractmethod
    def list(self, name: str) -> Iterator[ItemInfo]:
        """list the contents of <name>, non-recursively.

        Does not yield TMP_SUFFIX items - usually they are either not finished
        uploading or they are leftover crap from aborted uploads.
        """
