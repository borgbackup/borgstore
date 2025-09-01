"""
Tests package.
"""


def key(k: int) -> str:
    # Convenient way to generate test keys; uses 4 bytes for readability.
    return k.to_bytes(4, "big").hex()


def lkey(k: int) -> str:
    # Convenient way to generate test keys; uses 4 bytes for readability.
    return k.to_bytes(4, "little").hex()


def list_store_names_sorted(store, name: str, deleted: bool = False):
    # Store helper for tests that only need the names of directory members.
    return sorted(info.name for info in store.list(name, deleted=deleted))


def list_store_names(store, name: str, deleted: bool = False):
    # Store helper for tests that only need the names of directory members.
    return list(info.name for info in store.list(name, deleted=deleted))


def list_names(backend, name: str):
    # Backend helper for tests that only need the names of directory members.
    return sorted(info.name for info in backend.list(name))
