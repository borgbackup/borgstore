"""Tests for Store optional cache behavior."""

import pytest

from borgstore.backends.errors import ObjectNotFound
from borgstore.constants import DEL_SUFFIX
from borgstore.store import CacheMode, Store

LEVELS = {"data/": [2], "meta/": [1], "config/": [0]}


def make_store(tmp_path, *, cache=None, with_cache_backend=True):
    primary = (tmp_path / "primary").resolve()
    cache_root = (tmp_path / "cache").resolve()
    kwargs = {"url": primary.as_uri(), "levels": LEVELS}
    if cache is not None:
        kwargs["cache"] = cache
    if with_cache_backend:
        kwargs["cache_url"] = cache_root.as_uri()
    return Store(**kwargs), cache_root


def test_cache_disabled_by_default(tmp_path):
    store, cache_root = make_store(tmp_path, cache=None, with_cache_backend=False)
    store.create()
    try:
        with store:
            name, value = "data/00000000", b"abc"
            store.store(name, value)
            assert store.load(name) == value
    finally:
        store.destroy()
    assert not cache_root.exists()


def test_cache_aliases_and_invalid_value(tmp_path):
    store, _ = make_store(tmp_path, cache={"data/": "cache", "meta/": "MIRROR", "config/": "off"})
    assert store.cache["data/"] == CacheMode.C_CACHE
    assert store.cache["meta/"] == CacheMode.C_MIRROR
    assert store.cache["config/"] == CacheMode.C_OFF
    with pytest.raises(ValueError):
        make_store(tmp_path, cache={"data/": "on"})


def test_cache_misconfiguration(tmp_path):
    primary_url = (tmp_path / "primary").resolve().as_uri()
    cache_url = (tmp_path / "cache").resolve().as_uri()
    with pytest.raises(ValueError):
        make_store(tmp_path, cache={"data/": CacheMode.C_CACHE}, with_cache_backend=False)
    with pytest.raises(ValueError):
        Store(url=primary_url, levels=LEVELS, cache={"missing/": CacheMode.C_CACHE}, cache_url=cache_url)


def test_cache_off_only_without_backend_is_ok(tmp_path):
    store, _ = make_store(tmp_path, cache={"data/": "off"}, with_cache_backend=False)
    store.create()
    try:
        with store:
            name, value = "data/00000000", b"abc"
            store.store(name, value)
            assert store.load(name) == value
    finally:
        store.destroy()


def test_c_cache_read_through_and_partial_load(tmp_path):
    store, _ = make_store(tmp_path, cache={"data/": CacheMode.C_CACHE})
    store.create()
    try:
        with store:
            name, value = "data/00000000", b"0123456789"
            store.store(name, value)
            store._cache_invalidate(store.find(name))
            calls = {"load": 0}
            original_load = store.backend.load

            def wrapped(name, size=None, offset=0):
                calls["load"] += 1
                return original_load(name, size=size, offset=offset)

            store.backend.load = wrapped
            try:
                assert store.load(name) == value
                assert store.load(name, size=4, offset=2) == value[2:6]
            finally:
                store.backend.load = original_load
            assert calls["load"] == 1
    finally:
        store.destroy()


def test_c_mirror_reads_always_from_primary_and_populates_cache(tmp_path):
    store, _ = make_store(tmp_path, cache={"data/": CacheMode.C_MIRROR})
    store.create()
    try:
        with store:
            name, value = "data/00000000", b"abc"
            nested_name = store.find(name)
            store.backend.store(nested_name, value)
            calls = {"load": 0}
            original_load = store.backend.load

            def wrapped(name, size=None, offset=0):
                calls["load"] += 1
                return original_load(name, size=size, offset=offset)

            store.backend.load = wrapped
            try:
                assert store.load(name) == value
                assert store.load(name) == value
            finally:
                store.backend.load = original_load
            assert calls["load"] == 2
            assert store.cache_backend.load(nested_name) == value
    finally:
        store.destroy()


@pytest.mark.parametrize("mode", [CacheMode.C_CACHE, CacheMode.C_MIRROR])
def test_write_delete_and_soft_delete_mirror_cache_entries(tmp_path, mode):
    store, _ = make_store(tmp_path, cache={"data/": mode})
    store.create()
    try:
        with store:
            name, value = "data/00000000", b"abc"
            nested = store.find(name)
            store.store(name, value)
            assert store.cache_backend.load(nested) == value

            store.move(name, delete=True)
            with pytest.raises(ObjectNotFound):
                store.cache_backend.load(nested)
            assert store.cache_backend.load(nested + DEL_SUFFIX) == value

            store.move(name, undelete=True)
            assert store.cache_backend.load(nested) == value

            store.delete(name)
            with pytest.raises(ObjectNotFound):
                store.cache_backend.load(nested)
    finally:
        store.destroy()


def test_generic_rename_and_change_level_move_cache(tmp_path):
    levels = {"data/": [0, 1]}
    primary_url = (tmp_path / "primary").resolve().as_uri()
    cache_url = (tmp_path / "cache").resolve().as_uri()
    store = Store(url=primary_url, levels=levels, cache={"data/": CacheMode.C_CACHE}, cache_url=cache_url)
    store.create()
    try:
        with store:
            old_name, new_name, value = "data/00000000", "data/00000001", b"x"
            store.store(old_name, value)
            old_nested = store.find(old_name)
            store.move(old_name, new_name=new_name)
            new_nested = store.find(new_name)
            with pytest.raises(ObjectNotFound):
                store.cache_backend.load(old_nested)
            assert store.cache_backend.load(new_nested) == value

            store.move(new_name, change_level=True)
            changed_nested = store.find(new_name)
            assert store.cache_backend.load(changed_nested) == value
    finally:
        store.destroy()


def test_deleted_reads_use_del_cache_key(tmp_path):
    store, _ = make_store(tmp_path, cache={"data/": CacheMode.C_CACHE})
    store.create()
    try:
        with store:
            name, value = "data/00000000", b"abc"
            nested = store.find(name)
            store.store(name, value)
            store.move(name, delete=True)
            store._cache_invalidate(nested + DEL_SUFFIX)
            calls = {"load": 0}
            original_load = store.backend.load

            def wrapped(name, size=None, offset=0):
                calls["load"] += 1
                return original_load(name, size=size, offset=offset)

            store.backend.load = wrapped
            try:
                assert store.load(name, deleted=True) == value
                assert store.load(name, deleted=True) == value
            finally:
                store.backend.load = original_load
            assert calls["load"] == 1
            assert store.cache_backend.load(nested + DEL_SUFFIX) == value
    finally:
        store.destroy()


def test_cache_errors_do_not_fail_main_operations(tmp_path):
    store, _ = make_store(tmp_path, cache={"data/": CacheMode.C_CACHE})
    store.create()
    try:
        with store:
            name, value = "data/00000000", b"abc"
            store.cache_backend.store = lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom"))
            store.store(name, value)

            original_move = store.cache_backend.move
            store.cache_backend.move = lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom"))
            try:
                store.move(name, delete=True)
            finally:
                store.cache_backend.move = original_move

            assert store.info(name, deleted=True).exists
            assert store.stats["cache_errors"] >= 1
    finally:
        store.destroy()


def test_cache_stats(tmp_path):
    store, _ = make_store(tmp_path, cache={"data/": CacheMode.C_CACHE, "meta/": CacheMode.C_MIRROR})
    store.create()
    try:
        with store:
            data_name, data_value = "data/00000000", b"abc"
            meta_name, meta_value = "meta/00", b"xyz"
            store.store(data_name, data_value)
            store.load(data_name)
            store.load(data_name)
            store.store(meta_name, meta_value)
            store.load(meta_name)

            stats = store.stats
            assert stats["cache_hits"] == 2
            assert stats["cache_misses"] == 0
            assert stats["cache_bytes_read"] == 6
            assert stats["cache_bytes_written"] == 9
            assert stats["cache_disabled"] is False
            assert stats["cache_hit_ratio"] == 1.0
    finally:
        store.destroy()
