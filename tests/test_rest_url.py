import pytest
from borgstore.store import Store


def test_rest_url_info(tmp_path):
    repo_path = tmp_path / "repo"
    # Use rest: URL with stdio backend (empty host)
    url = f"rest:///{repo_path}"

    # Use levels=0 to avoid root nesting issues if they arise
    config = {"": {"levels": [0]}}
    store = Store(url, config=config)
    store.create()

    with store:
        item_name = "test-item"
        item_data = b"some data"
        store.store(item_name, item_data)

        # Test Store.info which calls Backend.info (HEAD)
        # This used to hang.
        info = store.info(item_name)
        assert info.exists
        assert info.size == len(item_data)
        assert info.atime > 0

        # Test listing
        items = list(store.list(""))
        assert len(items) == 1
        assert items[0].name == item_name
        assert items[0].atime > 0

        # Test nonexistent item
        # This also used to hang if it returned a 404 with a body.
        info_none = store.info("nonexistent")
        assert not info_none.exists
        assert info_none.size == 0

        # Test directory info (root)
        info_root = store.info("")
        assert info_root.exists
        assert info_root.directory

    store.destroy()
