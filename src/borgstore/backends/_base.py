"""
Base class and type definitions for all backend implementations in this package.

Docs that are not backend-specific are also found here.
"""

from abc import ABC, abstractmethod
from collections import namedtuple
from typing import Iterator

from ..constants import MAX_NAME_LENGTH, TMP_SUFFIX, HID_SUFFIX
from ..utils import hashing
from .errors import ReadRangeError

# atime is the last read access UNIX timestamp [s] or 0 if not implemented.
# mtime is the last modification UNIX timestamp [s] or 0 if not implemented - it must be
# stamped by the *storage side's* clock; backends that would only echo a client-supplied
# timestamp (e.g. rclone) must report 0 (unknown) instead.
ItemInfo = namedtuple("ItemInfo", "name exists size directory atime mtime", defaults=(0, 0))

# type of a value given to store: a memoryview is accepted in addition to bytes,
# so callers can avoid copying (e.g. give a slice of a big buffer they already have).
StoreValue = bytes | memoryview


def validate_value(value: StoreValue) -> StoreValue:
    """Validate/normalize a value given to store.

    bytes (and other bytes-like objects) are returned unchanged.
    A memoryview is cast to a 1-dimensional view of bytes, so len(value) always
    gives the number of bytes (even if the caller had a view with a bigger itemsize).
    """
    if isinstance(value, memoryview):
        try:
            return value.cast("B")
        except TypeError:
            # cast only works for C-contiguous views, but the backends need to
            # write the value out as one consecutive sequence of bytes.
            raise ValueError("value must be a C-contiguous memoryview") from None
    return value


def to_bytes(value: StoreValue) -> bytes:
    """Get a bytes object for code that can not deal with a memoryview.

    Note: this copies the data, except if it already is a bytes object.
    """
    return value if isinstance(value, bytes) else bytes(value)


def validate_sources(sources) -> list:
    """Validate the sources given to gather / defrag, return them as a list of (name, offset, size) tuples.

    Each source is a (name, offset, size) tuple (or list, e.g. when it comes from JSON):
    name is an item name [str], offset is an int (negative: counted from the end of the item),
    size is a non-negative int (the exact amount of bytes wanted).
    """
    # always build a new list and let the caller use it instead of <sources>: if <sources> is a
    # generator (or another iterator), the validation consumes it, so iterating over <sources>
    # again would silently yield nothing.
    result = []
    for source in sources:
        try:
            name, offset, size = source
        except (TypeError, ValueError):
            raise ValueError(f"source must be a (name, offset, size) tuple, got {source!r}") from None
        if not isinstance(name, str):
            raise ValueError(f"source name must be a str, got {name!r}")
        if not isinstance(offset, int) or isinstance(offset, bool):
            raise ValueError(f"source offset must be an int, got {offset!r}")
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise ValueError(f"source size must be a non-negative int, got {size!r}")
        result.append((name, offset, size))
    return result


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
    def store(self, name: str, value: StoreValue) -> None:
        """store <value> into <name>

        <value> is either bytes or a memoryview of bytes (see StoreValue).
        Backends must not keep a reference to a memoryview value after returning,
        because the caller may reuse or release the underlying buffer.
        """

    @abstractmethod
    def delete(self, name: str) -> None:
        """delete <name>"""

    @abstractmethod
    def move(self, curr_name: str, new_name: str) -> None:
        """rename curr_name to new_name (overwrite target)"""

    def gather(self, sources) -> bytes:
        """
        Read multiple byte ranges (from one or multiple items) and return their contents
        concatenated, in the order given.

        <sources> is a list of (name, offset, size) tuples, see validate_sources. The item names
        are backend names (with namespace and nesting, as for load). A short read raises
        ReadRangeError. For an empty list, b"" is returned.

        The caller knows the sizes of the requested ranges, so it can split the returned data
        (e.g. into memoryview slices).
        """
        # default implementation: one (partial) load per range, works for all backends.
        # might be overridden for performance (e.g. to save one roundtrip per range).
        sources = validate_sources(sources)
        data_parts = []
        for name, offset, size in sources:
            if size == 0:
                continue  # nothing to read (and some backends reject an empty range request)
            chunk = self.load(name, offset=offset, size=size)
            if len(chunk) != size:
                raise ReadRangeError(
                    f"Read range error from {name} (requested {size} bytes at offset {offset}, got {len(chunk)})"
                )
            data_parts.append(chunk)
        return b"".join(data_parts)

    def defrag(self, sources, *, target=None, algorithm=None, namespace=None, levels=0) -> str:
        """
        Similar to the higher-level Store.defrag method, with these differences:

        - source and target item names are with namespace.
        - if levels > 0, source and target item names are nested.

        <algorithm> can be any algorithm supported by hashlib or "blake3"
        (the latter requires the optional "blake3" package).

        Returns the target item name.
        """
        # default implementation: gather the ranges, then store them as a new item.
        # works for all backends, might be overridden (e.g. to run it remotely).
        from ..utils.nesting import nest

        data = self.gather(sources)
        if target is None:
            if algorithm is None:
                raise ValueError("Either target or algorithm must be given for defrag")
            h = hashing.new(algorithm)
            h.update(data)
            target = h.hexdigest()
            if namespace:
                target = namespace.rstrip("/") + "/" + target
            if levels:
                target = nest(target, levels)
        self.store(target, data)
        return target

    def hash(self, name: str, algorithm: str = "sha256") -> str:
        """compute full-file hex digest of <name> content using <algorithm>

        <algorithm> can be any algorithm supported by hashlib or "blake3"
        (the latter requires the optional "blake3" package).
        """
        # default implementation: slow, but works for all backends.
        # might be overridden for performance.
        h = hashing.new(algorithm)
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
