"""
Key/Value Store Implementation.
"""

from typing import Iterator, Optional

from .utils.nesting import nest
from .backends._base import ItemInfo, BackendBase
from .backends.errors import ObjectNotFound  # noqa
from .backends.posixfs import get_file_backend
from .backends.sftp import get_sftp_backend
from .constants import DEL_SUFFIX


def get_backend(url):
    """parse backend URL and return a backend instance (or None)"""
    backend = get_file_backend(url)
    if backend is not None:
        return backend

    backend = get_sftp_backend(url)
    if backend is not None:
        return backend


class Store:
    def __init__(self, url: Optional[str] = None, backend: Optional[BackendBase] = None, levels: Optional[dict] = None):
        levels = levels if levels else {}
        # we accept levels as a dict, but we rather want a list of (namespace, levels) tuples, longest namespace first:
        self.levels = [entry for entry in sorted(levels.items(), key=lambda item: len(item[0]), reverse=True)]
        if backend is None and url is not None:
            backend = get_backend(url)
            if backend is None:
                raise ValueError(f"Invalid Backend Storage URL: {url}")
        if backend is None:
            raise ValueError("You need to give a backend instance or a backend url.")
        self.backend = backend

    def create(self) -> None:
        self.backend.create()

    def destroy(self) -> None:
        self.backend.destroy()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def open(self) -> None:
        self.backend.open()

    def close(self) -> None:
        self.backend.close()

    def _get_levels(self, name):
        """get levels from configuration depending on namespace"""
        for namespace, levels in self.levels:
            if name.startswith(namespace):
                return levels
        return [0]  # "no nesting" is the default, if no namespace matched

    def find(self, name: str, *, deleted=False) -> str:
        """
        Find an item checking all supported nesting levels and return its nested name:

        - item not in the store yet:
          we won't find it, but find will return a nested name for **last** level.
        - item is in the store already:
          find will return the same nested name as the already present item.

        If deleted is True, find will try to find a "deleted" item.
        """
        nested_name = None
        suffix = DEL_SUFFIX if deleted else None
        for level in self._get_levels(name):
            nested_name = nest(name, level, add_suffix=suffix)
            info = self.backend.info(nested_name)
            if info.exists:
                break
        return nested_name

    def info(self, name: str, *, deleted=False) -> ItemInfo:
        return self.backend.info(self.find(name, deleted=deleted))

    def load(self, name: str, *, size=None, offset=0, deleted=False) -> bytes:
        return self.backend.load(self.find(name, deleted=deleted), size=size, offset=offset)

    def store(self, name: str, value: bytes) -> None:
        # note: using .find here will:
        # - overwrite an existing item (level stays same)
        # - write to the last level if no existing item is found.
        self.backend.store(self.find(name), value)

    def delete(self, name: str, *, deleted=False) -> None:
        """
        Really and immediately deletes an item.

        See also .move(name, delete=True) for "soft" deletion.
        """
        self.backend.delete(self.find(name, deleted=deleted))

    def move(
        self,
        name: str,
        new_name: Optional[str] = None,
        *,
        delete: bool = False,
        undelete: bool = False,
        change_level: bool = False,
        deleted: bool = False,
    ) -> None:
        if delete:
            # use case: keep name, but soft "delete" the item
            nested_name = self.find(name, deleted=False)
            nested_new_name = nested_name + DEL_SUFFIX
        elif undelete:
            # use case: keep name, undelete a previously soft "deleted" item
            nested_name = self.find(name, deleted=True)
            nested_new_name = nested_name.removesuffix(DEL_SUFFIX)
        elif change_level:
            # use case: keep name, changing to another nesting level
            suffix = DEL_SUFFIX if deleted else None
            nested_name = self.find(name, deleted=deleted)
            nested_new_name = nest(name, self._get_levels(name)[-1], add_suffix=suffix)
        else:
            # generic use (be careful!)
            if not new_name:
                raise ValueError("generic move needs new_name to be given.")
            nested_name = self.find(name, deleted=deleted)
            nested_new_name = self.find(new_name, deleted=deleted)
        self.backend.move(nested_name, nested_new_name)

    def list(self, name: str, deleted: bool = False) -> Iterator[ItemInfo]:
        """
        List all names in the namespace <name>.

        If deleted is True and soft deleted items are encountered, they are yielded
        as if they were not deleted. Otherwise, they are ignored.
        """
        # as the backend.list method only supports non-recursive listing and
        # also returns directories/namespaces we introduced for nesting, we do the
        # recursion here (and also we do not yield directory names from here).
        for info in self.backend.list(name):
            if info.directory:
                # note: we only expect subdirectories from key nesting, but not namespaces nested into each other.
                subdir_name = (name + "/" + info.name) if name else info.name
                yield from self.list(subdir_name, deleted=deleted)
            else:
                is_deleted = info.name.endswith(DEL_SUFFIX)
                if deleted and is_deleted:
                    yield info._replace(name=info.name.removesuffix(DEL_SUFFIX))
                elif not is_deleted:
                    yield info
