"""
Base class and type definitions for all backend implementations in this package.

Docs that are not backend-specific are also found here.
"""

import hashlib
from abc import ABC, abstractmethod
from collections import namedtuple
from typing import Iterator

from ..constants import MAX_NAME_LENGTH, TMP_SUFFIX, HID_SUFFIX

ItemInfo = namedtuple("ItemInfo", "name exists size directory")


def validate_name(name):
    """Validate a backend key/name."""
    # this is used before an object is accepted for storage and
    # it is also used before a name is returned by list method.
    # no crap in, no crap out (even if it is not from us).
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
    if name.endswith(TMP_SUFFIX):
        # TMP_SUFFIX is used for temporary files internally, e.g. while files are uploading.
        raise ValueError(f"name must not end with {TMP_SUFFIX}, but got: {name}")
    if name.endswith(HID_SUFFIX):
        # HID_SUFFIX is used for hidden internal files, not accessible by users.
        raise ValueError(f"name must not end with {HID_SUFFIX}, but got: {name}")


class BackendBase(ABC):
    # a backend can request all directories to be pre-created once at backend creation (initialization) time.
    # for some backends this will optimize the performance of store and move operation, because they won't
    # have to care for ad-hoc directory creation for every store or move call. of course, create will take
    # significantly longer, especially if nesting on levels > 1 is used.
    # otoh, for some backends this might be completely pointless, e.g. if mkdir is a NOP (is ignored).
    # for the unit tests, precreate_dirs should be set to False, otherwise they get slowed down too much.
    # for interactive usage, precreate_dirs = False is often the less annoying, quicker option.
    # code in .store and .move methods can deal with mkdir in the exception handler, after first just
    # assuming that the directory is usually already there.
    precreate_dirs: bool = False

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
        """load value from <name>

        If offset is negative, it is counted from the end of the file.
        If size is None, the whole object starting at offset is loaded.
        """

    @abstractmethod
    def store(self, name: str, value: bytes) -> None:
        """store <value> into <name>"""

    @abstractmethod
    def delete(self, name: str) -> None:
        """delete <name>"""

    @abstractmethod
    def move(self, curr_name: str, new_name: str) -> None:
        """rename curr_name to new_name (overwrite target)"""

    def defrag(self, sources, *, target=None, algorithm=None, namespace=None, levels=0) -> str:
        """
        Similar to the higher-level Store.defrag method, with these differences:

        - source and target item names are with namespace.
        - if levels > 0, source and target item names are nested.

        Returns the target item name.
        """
        # default implementation: slow, but works for all backends.
        # might be overridden for performance.
        from ..utils.nesting import nest

        data = b"".join(self.load(source, offset=offset, size=size) for source, offset, size in sources)
        if target is None:
            if algorithm is None:
                raise ValueError("Either target or algorithm must be given for defrag")
            try:
                h = hashlib.new(algorithm)
            except (ValueError, TypeError):
                raise ValueError(f"Unsupported hash algorithm: {algorithm}")
            h.update(data)
            target = h.hexdigest()
            if namespace:
                target = namespace.rstrip("/") + "/" + target
            if levels:
                target = nest(target, levels)
        self.store(target, data)
        return target

    def hash(self, name: str, algorithm: str = "sha256") -> str:
        """compute full-file hex digest of <name> content using <algorithm>"""
        # default implementation: slow, but works for all backends.
        # might be overridden for performance.
        try:
            h = hashlib.new(algorithm)
        except ValueError:
            raise ValueError(f"Unsupported hash algorithm: {algorithm}") from None
        h.update(self.load(name))
        return h.hexdigest()

    def quota(self) -> dict:
        """Return quota information: limit and usage in bytes. -1 means not set / not tracked."""
        return dict(limit=-1, usage=-1)

    @abstractmethod
    def list(self, name: str) -> Iterator[ItemInfo]:
        """list the contents of <name>, non-recursively.

        Does not yield TMP_SUFFIX items - usually they are either not finished
        uploading or they are leftover crap from aborted uploads.

        The yielded ItemInfos are sorted alphabetically by name.
        """
