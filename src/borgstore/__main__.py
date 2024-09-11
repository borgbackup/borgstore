"""
Demo for BorgStore
==================

Usage:  python -m borgstore <borgstore_storage_url>

E.g.:   python -m borgstore file:///tmp/borgstore_storage

Please be careful: the given storage will be created, used and **completely deleted**!
"""


def run_demo(storage_url):
    from .store import Store

    def id_key(data: bytes):
        from hashlib import new

        h = new("sha256", data)
        return f"data/{h.hexdigest()}"

    levels_config = {
        "config/": [0],  # no nesting needed/wanted for the configs
        "data/": [2],  # 2 nesting levels wanted for the data
    }
    store = Store(url=storage_url, levels=levels_config)
    try:
        store.create()
    except FileExistsError:
        # currently, we only have file:// storages, so this should be fine.
        print("Error: you must not give an existing directory.")
        return

    with store:
        print("Writing 2 items to config namespace...")
        settings1_key = "config/settings1"
        store.store(settings1_key, b"value1 = 42")
        settings2_key = "config/settings2"
        store.store(settings2_key, b"value2 = 23")

        print(f"Listing config namespace contents: {list(store.list('config'))}")

        settings1_value = store.load(settings1_key)
        print(f"Loaded from store: {settings1_key}: {settings1_value.decode()}")
        settings2_value = store.load(settings2_key)
        print(f"Loaded from store: {settings2_key}: {settings2_value.decode()}")

        print("Writing 2 items to data namespace...")
        data1 = b"some arbitrary binary data."
        key1 = id_key(data1)
        store.store(key1, data1)
        data2 = b"more arbitrary binary data. " * 2
        key2 = id_key(data2)
        store.store(key2, data2)
        print(f"Soft deleting item {key2} ...")
        store.move(key2, delete=True)

        print(f"Listing data namespace contents: {list(store.list('data', deleted=False))}")
        print(f"Listing data namespace contents, incl. deleted: {list(store.list('data', deleted=True))}")

        print(f"Stats: {store.stats}")

    answer = input("After you've inspected the storage, enter DESTROY to destroy the storage, anything else to abort: ")
    if answer == "DESTROY":
        store.destroy()


if __name__ == "__main__":
    import sys

    if len(sys.argv) == 2:
        run_demo(sys.argv[1])
    else:
        print(__doc__)
