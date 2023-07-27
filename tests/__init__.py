"""
Tests package.
"""


def key(k: int) -> str:
    # easy way to generate keys for testing, 4 bytes for better readability.
    return k.to_bytes(4, "big").hex()


def lkey(k: int) -> str:
    # easy way to generate keys for testing, 4 bytes for better readability.
    return k.to_bytes(4, "little").hex()


def list_store_names(store, name: str, deleted: bool = False):
    # Store helper for tests only interested in the **names** of directory members
    return sorted(info.name for info in store.list(name, deleted=deleted))


def list_names(backend, name: str):
    # Backend helper for tests only interested in the **names** of directory members
    return sorted(info.name for info in backend.list(name))
