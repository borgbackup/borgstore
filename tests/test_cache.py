"""Tests for Store optional cache behavior."""

import logging

import pytest
import borgstore.store as store_module

from borgstore.backends.errors import ObjectNotFound
from borgstore.constants import DEL_SUFFIX, ROOTNS
from borgstore.store import CacheMode, Store

BASE_LEVELS = {"data/": [2], "meta/": [1], "config/": [0]}
BASE_CONFIG = {"data/": {"levels": [2]}, "meta/": {"levels": [1]}, "config/": {"levels": [0]}}


def make_config(cache_overrides=None, **extra):
    """Build a full config dict, merging BASE_LEVELS with optional per-namespace cache settings.

    cache_overrides: dict mapping namespace -> dict of cache keys ("cache", "max_age", "size")
    """
    result = {}
    for ns, levels in BASE_LEVELS.items():
        ns_cfg = {"levels": levels}
        if cache_overrides and ns in cache_overrides:
            ns_cfg.update(cache_overrides[ns])
        result[ns] = ns_cfg
    return result


def make_store(tmp_path, *, config=None, with_cache_backend=True):
    primary = (tmp_path / "primary").resolve()
    cache_root = (tmp_path / "cache").resolve()
    if config is None:
        config = BASE_CONFIG
    kwargs = {"url": primary.as_uri(), "config": config}
    if with_cache_backend:
        kwargs["cache_url"] = cache_root.as_uri()
    return Store(**kwargs), cache_root


def fill_shared_cache(tmp_path, names_values):
    """Store items using another Store that shares the primary and the cache, but has no cache limits."""
    other, _ = make_store(tmp_path, config=make_config({"data/": {"cache": CacheMode.C_WRITETHROUGH}}))
    with other:
        for name, value in names_values:
            other.store(name, value)


def cache_usage(cache_root, namespace="data"):
    """Return the total size of the files the cache really has in namespace."""
    return sum(path.stat().st_size for path in (cache_root / namespace).rglob("*") if path.is_file())


def test_cache_store_memoryview(tmp_path):
    """A memoryview value is written to the primary backend as well as to the cache backend."""
    store, _ = make_store(tmp_path, config=make_config({"data/": {"cache": "writethrough"}}))
    store.create()
    try:
        with store:
            buffer = bytearray(b"0123456789" * 10)
            name, value = "data/00000000", memoryview(buffer)[10:30]
            store.store(name, value)
            assert store.stats["cache_store_calls"] == 1
            assert store.stats["cache_store_volume"] == 20
            # served from the cache:
            assert store.load(name) == bytes(value)
            assert store.stats["cache_hits"] == 1
            # ... and the primary backend has it, too:
            store.cache_invalidate(name)
            assert store.load(name) == bytes(value)
    finally:
        store.destroy()


def test_cache_disabled_by_default(tmp_path):
    store, cache_root = make_store(tmp_path, config=None, with_cache_backend=False)
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
    store, _ = make_store(
        tmp_path,
        config=make_config(
            {"data/": {"cache": "writethrough"}, "meta/": {"cache": "mirror"}, "config/": {"cache": "off"}}
        ),
    )
    store.create()
    try:
        with store:
            data_name, data_value = "data/00000000", b"abc"
            meta_name, meta_value = "meta/abcde", b"meta"
            config_name, config_value = "config/item", b"cfg"
            store.store(data_name, data_value)
            store.store(meta_name, meta_value)
            store.store(config_name, config_value)
            assert store.load(data_name) == data_value
            assert store.load(meta_name) == meta_value
            assert store.load(config_name) == config_value
    finally:
        store.destroy()
    with pytest.raises(ValueError):
        make_store(tmp_path, config={"data/": {"levels": [2], "cache": "on"}})


def test_cache_policy_dict_and_max_age_validation(tmp_path):
    store, _ = make_store(
        tmp_path,
        config=make_config(
            {
                "data/": {"cache": "writethrough", "max_age": 60, "size": 1024},
                "meta/": {"cache": "mirror"},
                "config/": {"cache": "off", "max_age": 0, "size": 0},
            }
        ),
    )
    store.create()
    try:
        with store:
            name, value = "data/00000000", b"abc"
            store.store(name, value)
            store.cache_invalidate(name)
            assert store.load(name) == value
            assert store.load(name) == value
            stats = store.stats
            assert stats["cache_misses"] == 1
            assert stats["cache_hits"] == 1
    finally:
        store.destroy()

    with pytest.raises(ValueError):
        make_store(tmp_path, config={"data/": {"max_age": 1}})  # missing levels
    with pytest.raises(ValueError):
        make_store(tmp_path, config=make_config({"data/": {"cache": "writethrough", "max_age": -1}}))
    with pytest.raises(ValueError):
        make_store(tmp_path, config=make_config({"data/": {"cache": "writethrough", "max_age": "1"}}))
    with pytest.raises(ValueError):
        make_store(tmp_path, config=make_config({"data/": {"cache": "writethrough", "size": -1}}))
    with pytest.raises(ValueError):
        make_store(tmp_path, config=make_config({"data/": {"cache": "writethrough", "size": 1.5}}))
    with pytest.raises(ValueError):
        make_store(tmp_path, config=make_config({"data/": {"cache": "writethrough", "size": "1"}}))
    with pytest.raises(ValueError):
        make_store(tmp_path, config=make_config({"data/": {"cache": "writethrough", "unexpected": 1}}))
    with pytest.raises(ValueError):
        make_store(tmp_path, config="bad")  # config itself must be a dict
    with pytest.raises(ValueError):
        make_store(tmp_path, config={"data/": "bad"})  # ns_config must be a dict


def test_cache_mode_enum_input(tmp_path):
    """CacheMode enum values are accepted as the 'cache' key in namespace config."""
    store, _ = make_store(
        tmp_path, config=make_config({"data/": {"cache": CacheMode.C_WRITETHROUGH, "max_age": 60.0, "size": 512}})
    )
    store.create()
    try:
        with store:
            name, value = "data/00000000", b"abc"
            store.store(name, value)
            store.cache_invalidate(name)
            assert store.load(name) == value
            assert store.load(name) == value
            stats = store.stats
            assert stats["cache_misses"] == 1
            assert stats["cache_hits"] == 1
    finally:
        store.destroy()


def test_cache_misconfiguration(tmp_path):
    with pytest.raises(ValueError):
        # cache enabled but no cache backend given
        make_store(tmp_path, config=make_config({"data/": {"cache": "writethrough"}}), with_cache_backend=False)


def test_cache_off_only_without_backend_is_ok(tmp_path):
    store, _ = make_store(tmp_path, config=make_config({"data/": {"cache": "off"}}), with_cache_backend=False)
    store.create()
    try:
        with store:
            name, value = "data/00000000", b"abc"
            store.store(name, value)
            assert store.load(name) == value
    finally:
        store.destroy()


def test_c_cache_read_through_and_partial_load(tmp_path):
    store, _ = make_store(tmp_path, config=make_config({"data/": {"cache": CacheMode.C_WRITETHROUGH}}))
    store.create()
    try:
        with store:
            name, value = "data/00000000", b"0123456789"
            store.store(name, value)
            store.cache_invalidate(name)

            assert store.load(name) == value
            assert store.load(name, size=4, offset=2) == value[2:6]

            stats = store.stats
            assert stats["cache_load_calls"] == 2
            assert stats["cache_hits"] == 1
            assert stats["cache_misses"] == 1
            # the cache hit was served by a partial read: only the requested 4 bytes were loaded.
            assert stats["cache_load_volume"] == 4
    finally:
        store.destroy()


def test_c_cache_hit_uses_partial_read(tmp_path):
    """On a cache hit, the requested range is read partially from the cache, not fully loaded."""
    store, _ = make_store(tmp_path, config=make_config({"data/": {"cache": CacheMode.C_WRITETHROUGH}}))
    store.create()
    try:
        with store:
            name, value = "data/00000000", b"0123456789"
            store.store(name, value)  # populates the cache (writethrough)

            calls = []
            orig_load = store.cache_backend.load

            def spy_load(nested_name, *, size=None, offset=0):
                calls.append((size, offset))
                return orig_load(nested_name, size=size, offset=offset)

            store.cache_backend.load = spy_load
            assert store.load(name, size=4, offset=2) == value[2:6]
            assert calls == [(4, 2)]
    finally:
        store.destroy()


def test_c_mirror_reads_always_from_primary_and_populates_cache(tmp_path):
    store, _ = make_store(tmp_path, config=make_config({"data/": {"cache": CacheMode.C_MIRROR}}))
    store.create()
    try:
        with store:
            name, value = "data/00000000", b"abc"
            nested_name = store.find(name)
            store.backend.store(nested_name, value)

            assert store.load(name) == value
            assert store.load(name) == value

            stats = store.stats
            assert stats["cache_load_calls"] == 0
            assert stats["cache_store_calls"] == 2
            assert stats["load_calls"] == 2
            assert store.cache_backend.load(nested_name) == value
    finally:
        store.destroy()


@pytest.mark.parametrize("mode", [CacheMode.C_WRITETHROUGH, CacheMode.C_MIRROR])
def test_write_delete_and_soft_delete_mirror_cache_entries(tmp_path, mode):
    store, _ = make_store(tmp_path, config=make_config({"data/": {"cache": mode}}))
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
    primary_url = (tmp_path / "primary").resolve().as_uri()
    cache_url = (tmp_path / "cache").resolve().as_uri()
    store = Store(
        url=primary_url, config={"data/": {"levels": [0, 1], "cache": CacheMode.C_WRITETHROUGH}}, cache_url=cache_url
    )
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
    store, _ = make_store(tmp_path, config=make_config({"data/": {"cache": CacheMode.C_WRITETHROUGH}}))
    store.create()
    try:
        with store:
            name, value = "data/00000000", b"abc"
            nested = store.find(name)
            store.store(name, value)
            store.move(name, delete=True)
            store.cache_invalidate(name, deleted=True)

            assert store.load(name, deleted=True) == value
            assert store.load(name, deleted=True) == value

            stats = store.stats
            assert stats["cache_load_calls"] == 2
            assert stats["cache_hits"] == 1
            assert stats["cache_misses"] == 1
            assert store.cache_backend.load(nested + DEL_SUFFIX) == value
    finally:
        store.destroy()


def test_c_cache_does_not_expire_on_read_but_on_open(tmp_path, monkeypatch):
    store, _ = make_store(tmp_path, config=make_config({"data/": {"cache": CacheMode.C_WRITETHROUGH, "max_age": 5}}))
    store.create()
    try:
        with store:
            name, value = "data/00000000", b"abc"
            store.store(name, value)
            nested_name = store.find(name)
            store.cache_invalidate(name)

            now = 1000.0
            atime = 0.0

            def fake_time():
                return now

            monkeypatch.setattr("borgstore.store.time.time", fake_time)
            original_info = store.cache_backend.info

            def wrapped_info(backend_name):
                info = original_info(backend_name)
                if backend_name == nested_name and info.exists:
                    return info._replace(atime=atime)
                return info

            store.cache_backend.info = wrapped_info

            try:
                assert store.load(name) == value  # miss, populate cache at t=1000
                atime = 1000.0
                now = 1004.0
                assert store.load(name) == value  # hit
                now = 1010.0  # past max_age (5s)
                assert store.load(name) == value  # still hit (lazy/no read-time expiration)
            finally:
                store.cache_backend.info = original_info

            stats = store.stats
            assert stats["cache_misses"] == 1
            assert stats["cache_hits"] == 2
            assert stats["cache_load_calls"] == 3
            assert stats["cache_delete_calls"] == 1
    finally:
        store.destroy()


def test_cache_errors_do_not_fail_main_operations(tmp_path):
    store, _ = make_store(tmp_path, config=make_config({"data/": {"cache": CacheMode.C_WRITETHROUGH}}))
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
    store, _ = make_store(
        tmp_path,
        config=make_config({"data/": {"cache": CacheMode.C_WRITETHROUGH}, "meta/": {"cache": CacheMode.C_MIRROR}}),
    )
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
            assert stats["cache_load_calls"] == 2
            assert stats["cache_store_calls"] == 3
            assert stats["cache_load_volume"] == 6
            assert stats["cache_store_volume"] == 9
            assert stats["cache_disabled"] is False
            assert stats["cache_hit_ratio"] == 1.0
    finally:
        store.destroy()


def test_open_cleans_up_expired_cache_items(tmp_path, monkeypatch):
    store, _ = make_store(tmp_path, config=make_config({"data/": {"cache": CacheMode.C_WRITETHROUGH, "max_age": 5}}))
    store.create()
    name, value = "data/00000000", b"abc"
    with store:
        store.store(name, value)
        nested_name = store.find(name)

    now = 2000.0
    atime = 1990.0

    def fake_time():
        return now

    monkeypatch.setattr("borgstore.store.time.time", fake_time)
    original_list = store.cache_backend.list

    def wrapped_list(backend_name):
        for info in original_list(backend_name):
            full_name = (backend_name + "/" + info.name) if backend_name else info.name
            if full_name == nested_name and info.exists:
                yield info._replace(atime=atime)
            else:
                yield info

    store.cache_backend.list = wrapped_list
    try:
        store.open()
        assert store.stats["cache_delete_calls"] == 1
        store.close()

        # Let's test non-expired case on next open
        atime = 1999.0
        # Reset count
        store._stats["cache_delete_calls"] = 0
        store.open()
        assert store.stats["cache_delete_calls"] == 0
        store.close()
    finally:
        store.cache_backend.list = original_list
        store.destroy()


def test_close_cleans_up_lru_cache_items_by_size(tmp_path, monkeypatch):
    store, _ = make_store(tmp_path, config=make_config({"data/": {"cache": CacheMode.C_WRITETHROUGH, "size": 7}}))
    store.create()
    names_values = [("data/00000000", b"aaaa"), ("data/00000001", b"bbbb"), ("data/00000002", b"cccc")]
    store.open()
    # another client sharing the cache fills it, so this store only finds the items when it scans at close time.
    fill_shared_cache(tmp_path, names_values)
    nested_names = [store.find(name) for name, _value in names_values]

    atimes = {nested_names[0]: 100.0, nested_names[1]: 200.0, nested_names[2]: 300.0}
    sizes = {nested_name: 4 for nested_name in nested_names}

    monkeypatch.setattr("borgstore.store.time.time", lambda: 500.0)
    original_list = store.cache_backend.list
    deleted_names = []
    original_delete = store.cache_backend.delete

    def wrapped_list(backend_name):
        for info in original_list(backend_name):
            full_name = (backend_name + "/" + info.name) if backend_name else info.name
            if full_name in atimes and info.exists:
                yield info._replace(atime=atimes[full_name], size=sizes[full_name])
            else:
                yield info

    def wrapped_delete(backend_name):
        if backend_name in atimes:
            deleted_names.append(backend_name)
        return original_delete(backend_name)

    store.cache_backend.list = wrapped_list
    store.cache_backend.delete = wrapped_delete
    try:
        store.close()
        assert deleted_names == [nested_names[0], nested_names[1]]
    finally:
        store.cache_backend.list = original_list
        store.cache_backend.delete = original_delete
        store.destroy()


def test_close_cleans_up_expired_before_lru_size_eviction(tmp_path, monkeypatch):
    store, _ = make_store(
        tmp_path, config=make_config({"data/": {"cache": CacheMode.C_WRITETHROUGH, "max_age": 50, "size": 7}})
    )
    store.create()
    names_values = [("data/00000000", b"aaaa"), ("data/00000001", b"bbbb"), ("data/00000002", b"cccc")]
    store.open()
    # another client sharing the cache fills it, so this store only finds the items when it scans at close time.
    fill_shared_cache(tmp_path, names_values)
    nested_names = [store.find(name) for name, _value in names_values]

    now = 1000.0
    atimes = {nested_names[0]: 900.0, nested_names[1]: 990.0, nested_names[2]: 995.0}
    sizes = {nested_name: 4 for nested_name in nested_names}

    monkeypatch.setattr("borgstore.store.time.time", lambda: now)
    original_list = store.cache_backend.list
    deleted_names = []
    original_delete = store.cache_backend.delete

    def wrapped_list(backend_name):
        for info in original_list(backend_name):
            full_name = (backend_name + "/" + info.name) if backend_name else info.name
            if full_name in atimes and info.exists:
                yield info._replace(atime=atimes[full_name], size=sizes[full_name])
            else:
                yield info

    def wrapped_delete(backend_name):
        if backend_name in atimes:
            deleted_names.append(backend_name)
        return original_delete(backend_name)

    store.cache_backend.list = wrapped_list
    store.cache_backend.delete = wrapped_delete
    try:
        store.close()
        assert deleted_names == [nested_names[0], nested_names[1]]
    finally:
        store.cache_backend.list = original_list
        store.cache_backend.delete = original_delete
        store.destroy()


def test_open_cleanup_errors_are_best_effort(tmp_path):
    store, _ = make_store(tmp_path, config=make_config({"data/": {"cache": CacheMode.C_WRITETHROUGH, "max_age": 5}}))
    store.create()
    name, value = "data/00000000", b"abc"
    with store:
        store.store(name, value)

    original_list = store.cache_backend.list
    close_calls = {"count": 0}
    original_close = store.cache_backend.close

    def failing_list(_backend_name):
        raise RuntimeError("boom")
        yield  # pragma: no cover

    def wrapped_close():
        close_calls["count"] += 1
        return original_close()

    store.cache_backend.list = failing_list
    store.cache_backend.close = wrapped_close
    try:
        store.open()
        store.close()
        assert close_calls["count"] == 1
        assert store.stats["cache_errors"] >= 1
    finally:
        store.cache_backend.list = original_list
        store.cache_backend.close = original_close
        store.destroy()


def test_latency_emulation_not_applied_to_cache_backend_calls(tmp_path, monkeypatch):
    monkeypatch.setenv("BORGSTORE_LATENCY", "200000")
    store, _ = make_store(tmp_path, config=make_config({"data/": {"cache": CacheMode.C_WRITETHROUGH}}))
    store.create()
    try:
        with store:
            name, value = "data/00000000", b"abc"
            store.store(name, value)

            store.cache_invalidate(name)

            sleep_calls = []
            original_sleep = store_module.time.sleep

            def wrapped_sleep(seconds):
                sleep_calls.append(seconds)

            monkeypatch.setattr("borgstore.store.time.sleep", wrapped_sleep)
            try:
                assert store.load(name) == value
                stats = store.stats
                assert stats["backend_load_calls"] == 1
                sleeps_after_miss = len(sleep_calls)
                assert sleeps_after_miss >= 1

                assert store.load(name) == value
                stats = store.stats
                assert stats["backend_load_calls"] == 1
                # cache hit: no primary backend calls at all, so no new sleeps
                assert len(sleep_calls) == sleeps_after_miss
            finally:
                monkeypatch.setattr("borgstore.store.time.sleep", original_sleep)
    finally:
        store.destroy()


def test_bandwidth_emulation_not_applied_to_cache_backend_calls(tmp_path, monkeypatch):
    monkeypatch.setenv("BORGSTORE_LATENCY", "0")
    monkeypatch.setenv("BORGSTORE_BANDWIDTH", "8")  # 1 byte/s
    store, _ = make_store(tmp_path, config=make_config({"data/": {"cache": CacheMode.C_WRITETHROUGH}}))
    store.create()
    try:
        with store:
            name, value = "data/00000000", b"abc"
            store.store(name, value)
            store.cache_invalidate(name)

            sleep_calls = []
            original_sleep = store_module.time.sleep

            def wrapped_sleep(seconds):
                sleep_calls.append(seconds)

            monkeypatch.setattr("borgstore.store.time.sleep", wrapped_sleep)
            try:
                assert store.load(name) == value
                stats = store.stats
                assert stats["backend_load_calls"] == 1
                sleeps_after_miss = len(sleep_calls)
                assert any(seconds >= 2.9 for seconds in sleep_calls)

                assert store.load(name) == value
                stats = store.stats
                assert stats["backend_load_calls"] == 1
                assert len(sleep_calls) == sleeps_after_miss
            finally:
                monkeypatch.setattr("borgstore.store.time.sleep", original_sleep)
    finally:
        store.destroy()


def test_public_cache_invalidate(tmp_path):
    store, _ = make_store(
        tmp_path,
        config=make_config(
            {"data/": {"cache": CacheMode.C_WRITETHROUGH}, "meta/": {"cache": CacheMode.C_WRITETHROUGH}}
        ),
    )
    store.create()
    try:
        with store:
            name1 = "data/00000000"
            name2 = "data/00000001"
            name3 = "meta/abcde"
            val1, val2, val3 = b"val1", b"val2", b"val3"

            # Scenario 1: Invalidate a single item
            store.store(name1, val1)
            # Verify cache hit
            assert store.load(name1) == val1
            before_stats = store.stats
            assert before_stats["cache_hits"] == 1
            assert before_stats["cache_misses"] == 0

            # Invalidate name1
            store.cache_invalidate(name1)

            # Loading name1 should miss and fetch from primary backend
            assert store.load(name1) == val1
            after_stats = store.stats
            assert after_stats["cache_hits"] == 1  # unchanged
            assert after_stats["cache_misses"] == 1  # incremented

            # Scenario 2: Invalidate a namespace (e.g. "data/")
            store.store(name1, val1)
            store.store(name2, val2)
            store.store(name3, val3)

            # Prime hits for all
            assert store.load(name1) == val1
            assert store.load(name2) == val2
            assert store.load(name3) == val3

            stats_before_ns = store.stats

            # Invalidate "data/" namespace
            store.cache_invalidate("data/")

            # Reads on "data/" items should miss, meta item should still hit
            assert store.load(name1) == val1
            assert store.load(name2) == val2
            assert store.load(name3) == val3

            stats_after_ns = store.stats
            assert stats_after_ns["cache_misses"] == stats_before_ns["cache_misses"] + 2
            assert stats_after_ns["cache_hits"] == stats_before_ns["cache_hits"] + 1  # meta/abcde hit

            # Scenario 3: Invalidate root/all namespaces using ROOTNS
            store.store(name1, val1)
            store.store(name3, val3)
            # Prime hits
            assert store.load(name1) == val1
            assert store.load(name3) == val3

            stats_before_root = store.stats

            # Invalidate all (ROOTNS)
            store.cache_invalidate(ROOTNS)

            assert store.load(name1) == val1
            assert store.load(name3) == val3

            stats_after_root = store.stats
            assert stats_after_root["cache_misses"] == stats_before_root["cache_misses"] + 2
            assert stats_after_root["cache_hits"] == stats_before_root["cache_hits"]  # no new hits

            # Scenario 4: Verify that omitting the name parameter raises TypeError (mandatory parameter)
            with pytest.raises(TypeError):
                store.cache_invalidate()  # type: ignore[call-overload]
    finally:
        store.destroy()


def make_limited_store(tmp_path, **limits):
    """Return (store, cache_root), data/ is cached in writethrough mode with the given limits."""
    return make_store(tmp_path, config=make_config({"data/": {"cache": CacheMode.C_WRITETHROUGH, **limits}}))


def data_name(i):
    return f"data/{i:08d}"


def test_cache_size_limit_holds_while_loading(tmp_path):
    """Loading more than the cache size must not grow the cache beyond its size limit, #183."""
    names_values = [(data_name(i), bytes([i]) * 100) for i in range(10)]
    store, cache_root = make_limited_store(tmp_path, size=350)
    store.create()
    try:
        with make_store(tmp_path, with_cache_backend=False)[0] as uncached:
            for name, value in names_values:
                uncached.store(name, value)
        with store:
            for name, value in names_values:
                assert store.load(name) == value
                assert cache_usage(cache_root) <= 350
            assert cache_usage(cache_root) == 300
            assert store._cache_indexes["data/"].total == 300
            # the most recently loaded items are still cached:
            hits = store.stats["cache_hits"]
            for name, value in names_values[-3:]:
                assert store.load(name) == value
            assert store.stats["cache_hits"] == hits + 3
    finally:
        store.destroy()


def test_cache_size_limit_holds_while_storing(tmp_path):
    store, cache_root = make_limited_store(tmp_path, size=350)
    store.create()
    try:
        with store:
            for i in range(10):
                store.store(data_name(i), bytes([i]) * 100)
                assert cache_usage(cache_root) <= 350
            assert cache_usage(cache_root) == 300
    finally:
        store.destroy()


def test_cache_evicts_least_recently_used_of_this_session(tmp_path):
    """The eviction order follows this store's cache hits, it does not depend on the backend's atime."""
    store, cache_root = make_limited_store(tmp_path, size=300)
    store.create()
    try:
        with store:
            for i in range(3):
                store.store(data_name(i), bytes([i]) * 100)
            assert store.load(data_name(0)) == bytes([0]) * 100  # hit: item 1 is the least recently used now
            store.store(data_name(3), bytes([3]) * 100)
            cached = {info.name for info in store._cache_list("data")}
            assert cached == {store.find(data_name(i)) for i in (0, 2, 3)}
    finally:
        store.destroy()


def test_cache_partial_load_miss_accounts_full_item(tmp_path):
    store, cache_root = make_limited_store(tmp_path, size=250)
    store.create()
    try:
        with make_store(tmp_path, with_cache_backend=False)[0] as uncached:
            for i in range(3):
                uncached.store(data_name(i), bytes([i]) * 100)
        with store:
            for i in range(3):
                assert store.load(data_name(i), offset=10, size=5) == bytes([i]) * 5
                assert store._cache_indexes["data/"].total == cache_usage(cache_root) <= 250
            assert cache_usage(cache_root) == 200
    finally:
        store.destroy()


def test_cache_does_not_cache_items_bigger_than_size(tmp_path):
    store, cache_root = make_limited_store(tmp_path, size=100)
    store.create()
    try:
        with store:
            small, big = b"s" * 60, b"b" * 101
            store.store(data_name(0), small)
            store.store(data_name(1), big)  # can not fit, must not evict anything either
            assert cache_usage(cache_root) == 60
            assert store.load(data_name(1)) == big
            assert store.load(data_name(1), offset=1, size=2) == b"bb"
            assert cache_usage(cache_root) == 60
            # a big value replacing a small one must not leave the previous value in the cache:
            store.store(data_name(0), big)
            assert cache_usage(cache_root) == 0
            assert store.load(data_name(0)) == big
            assert store.stats["cache_errors"] == 0
    finally:
        store.destroy()


def test_cache_index_accounting(tmp_path):
    """The index total follows overwrite, delete, soft delete / undelete and invalidation."""
    store, cache_root = make_limited_store(tmp_path, size=1000)
    store.create()
    try:
        with store:
            index = store._cache_indexes["data/"]
            store.store(data_name(0), b"a" * 100)
            store.store(data_name(1), b"b" * 100)
            assert index.total == cache_usage(cache_root) == 200
            store.store(data_name(0), b"a" * 30)  # overwrite
            assert index.total == cache_usage(cache_root) == 130
            store.move(data_name(0), delete=True)
            assert index.total == cache_usage(cache_root) == 130
            assert store.find(data_name(0), deleted=True) in index.entries
            assert store.find(data_name(0)) not in index.entries
            store.move(data_name(0), undelete=True)
            assert index.total == cache_usage(cache_root) == 130
            assert store.find(data_name(0)) in index.entries
            store.delete(data_name(0))
            assert index.total == cache_usage(cache_root) == 100
            store.cache_invalidate("data/")
            assert index.total == cache_usage(cache_root) == 0
            assert not index.entries
    finally:
        store.destroy()


def test_cache_shared_item_evicted_by_other_client(tmp_path):
    """If another client evicted an item, that is a cache miss, the item gets cached again."""
    store, cache_root = make_limited_store(tmp_path, size=1000)
    store.create()
    try:
        with store:
            index = store._cache_indexes["data/"]
            store.store(data_name(0), b"a" * 100)
            store.store(data_name(1), b"b" * 100)
            (cache_root / store.find(data_name(0))).unlink()
            assert store.load(data_name(0)) == b"a" * 100
            assert store.stats["cache_misses"] == 1
            assert store.stats["cache_errors"] == 0
            assert index.total == cache_usage(cache_root) == 200
    finally:
        store.destroy()


def test_cache_shared_hit_on_item_of_other_client(tmp_path):
    """A cache hit on an item another client has cached adds the full item to the index."""
    store, cache_root = make_limited_store(tmp_path, size=1000)
    store.create()
    try:
        with store:
            index = store._cache_indexes["data/"]
            fill_shared_cache(tmp_path, [(data_name(0), b"a" * 100)])
            assert index.total == 0
            assert store.load(data_name(0), offset=0, size=10) == b"a" * 10
            assert store.stats["cache_hits"] == 1
            assert index.entries[store.find(data_name(0))][0] == 100
            assert index.total == 100
    finally:
        store.destroy()


def test_cache_shared_rescan_sees_other_clients_items(tmp_path, monkeypatch):
    """After inserting more than size / CACHE_RESCAN_DIVISOR bytes, the store scans the shared cache."""
    monkeypatch.setattr(store_module, "CACHE_SCAN_STEP_TIME", 60)  # scan it in one step, also if the machine is slow
    store, cache_root = make_limited_store(tmp_path, size=1000)
    store.create()
    try:
        with store:
            index = store._cache_indexes["data/"]
            # another client fills the cache up to the limit:
            fill_shared_cache(tmp_path, [(data_name(100 + i), b"o" * 100) for i in range(10)])
            assert cache_usage(cache_root) == 1000
            for i in range(10):
                store.store(data_name(i), bytes([i]) * 100)
                # this store can only overshoot by what it inserts between 2 scans:
                assert cache_usage(cache_root) <= 1000 + 1000 // store_module.CACHE_RESCAN_DIVISOR + 100
            assert index.total == cache_usage(cache_root) <= 1000
            # the other client's items were the least recently used ones, so they got evicted first:
            cached = {info.name for info in store._cache_list("data")}
            assert {store.find(data_name(i)) for i in range(10)} <= cached
    finally:
        store.destroy()


def test_cache_max_age_eviction_when_storing(tmp_path, monkeypatch):
    """Expired items get evicted when something is put into the cache, a cache hit never expires anything."""
    now = 1000.0
    monkeypatch.setattr("borgstore.store.time.time", lambda: now)
    store, cache_root = make_limited_store(tmp_path, max_age=5)
    store.create()
    try:
        with store:
            store.store(data_name(0), b"a" * 10)
            now = 1004.0
            store.store(data_name(1), b"b" * 10)
            now = 1007.0  # item 0 is expired now
            assert store.load(data_name(0)) == b"a" * 10
            assert store.stats["cache_hits"] == 1  # still served from the cache, and that counts as using it
            now = 1010.0  # item 1 is expired now, item 0 was used 3s ago
            store.store(data_name(2), b"c" * 10)
            cached = {info.name for info in store._cache_list("data")}
            assert cached == {store.find(data_name(i)) for i in (0, 2)}
    finally:
        store.destroy()


def test_cache_eviction_errors_do_not_fail_main_operations(tmp_path):
    store, cache_root = make_limited_store(tmp_path, size=250)
    store.create()
    try:
        with store:

            def failing_delete(_backend_name):
                raise RuntimeError("boom")

            original_delete = store.cache_backend.delete
            store.cache_backend.delete = failing_delete
            try:
                for i in range(5):
                    store.store(data_name(i), bytes([i]) * 100)
                for i in range(5):
                    assert store.load(data_name(i)) == bytes([i]) * 100
                assert store.stats["cache_errors"] >= 1
            finally:
                store.cache_backend.delete = original_delete
        # closing the store scans the cache and evicts what could not be evicted before:
        assert cache_usage(cache_root) <= 250
    finally:
        store.destroy()


def test_cache_without_limits_keeps_no_index(tmp_path):
    store, cache_root = make_store(tmp_path, config=make_config({"data/": {"cache": CacheMode.C_MIRROR}}))
    store.create()
    try:
        with store:
            assert store._cache_indexes == {}
            for i in range(5):
                store.store(data_name(i), bytes([i]) * 100)
            assert cache_usage(cache_root) == 500
        assert cache_usage(cache_root) == 500
    finally:
        store.destroy()


def test_cache_first_open_does_not_count_errors(tmp_path):
    """Scanning a namespace the cache backend does not have yet is not an error."""
    store, cache_root = make_limited_store(tmp_path, size=1000)
    store.create()
    try:
        with store:
            pass
        assert store.stats["cache_errors"] == 0
    finally:
        store.destroy()


def test_cache_scan_in_steps(tmp_path, monkeypatch, caplog):
    """While the store is in use, a scan is done in steps: each item put into the cache continues it."""
    monkeypatch.setattr(store_module, "CACHE_SCAN_STEP_TIME", 0)  # a step lists one item or directory
    store, cache_root = make_limited_store(tmp_path, size=1000)
    store.create()
    try:
        with store:
            index = store._cache_indexes["data/"]
            fill_shared_cache(tmp_path, [(data_name(100 + i), b"o" * 100) for i in range(10)])
            scanning = []
            with caplog.at_level(logging.DEBUG, logger="borgstore.store"):
                for i in range(100):
                    store.store(data_name(i), bytes([i]) * 100)
                    assert store.load(data_name(i)) == bytes([i]) * 100
                    scanning.append(index.scan is not None)
                    if True in scanning and not scanning[-1]:
                        break
            assert scanning.count(True) > 1  # the scan took multiple steps ...
            assert not scanning[-1]  # ... and was finished
            # the store knows the other client's items now and has made room for its own item:
            assert index.total == cache_usage(cache_root) <= 1000
            messages = [record.getMessage() for record in caplog.records]
            messages = [message for message in messages if "cache scan of namespace 'data/'" in message]
            assert len(messages) == 1
            assert "10 new, 0 gone" in messages[0]
            assert f"{scanning.count(True) + 1} steps" in messages[0]
    finally:
        store.destroy()


def test_cache_scan_in_steps_keeps_changes_by_this_store(tmp_path, monkeypatch):
    """What the store changes while a scan is in progress is not overridden by what the scan has (not) seen."""
    monkeypatch.setattr(store_module, "CACHE_SCAN_STEP_TIME", 0)  # a step lists one item or directory
    store, cache_root = make_limited_store(tmp_path, size=10000)
    store.create()
    try:
        with store:
            index = store._cache_indexes["data/"]
            for i in range(3):
                store.store(data_name(i), bytes([i]) * 100)
            fill_shared_cache(tmp_path, [(data_name(9), b"o" * 100)])  # another client caches an item
            nested = {i: store.find(data_name(i)) for i in (0, 1, 2, 3, 9)}
            # all items are in the same directory. the scan lists 2 directories, then the items 0, 1, 2 and 9.
            for _ in range(3):
                store._cache_scan(index, max_time=0)
            assert set(index.scan.seen) == {nested[0]}
            store.delete(data_name(0))  # the scan has seen this item
            (cache_root / nested[2]).unlink()  # another client evicts an item the scan has not seen yet
            # the scan has listed the directory already, so it will not see this item. storing it continues the scan.
            store.store(data_name(3), b"n" * 100)
            assert set(index.scan.seen) == {nested[0], nested[1]}
            while index.scan is not None:
                store._cache_scan(index, max_time=0)
            assert set(index.entries) == {nested[1], nested[3], nested[9]}
            assert index.total == cache_usage(cache_root) == 300
    finally:
        store.destroy()


def test_close_with_a_scan_in_progress(tmp_path, monkeypatch):
    monkeypatch.setattr(store_module, "CACHE_SCAN_STEP_TIME", 0)  # a step lists one item or directory
    store, cache_root = make_limited_store(tmp_path, size=1000)
    store.create()
    try:
        with store:
            index = store._cache_indexes["data/"]
            for i in range(7):
                store.store(data_name(i), bytes([i]) * 100)
            assert index.scan is not None and index.scan.seen  # the scan has listed the directory of the items
            # another client fills the cache. the scan in progress does not see these items any more.
            fill_shared_cache(tmp_path, [(data_name(100 + i), b"o" * 100) for i in range(10)])
            assert cache_usage(cache_root) == 1700
        # closing the store scanned the cache from the start and cleaned it up:
        assert cache_usage(cache_root) == 1000
        assert store.stats["cache_errors"] == 0
    finally:
        store.destroy()


def test_cache_scan_errors_do_not_fail_main_operations(tmp_path):
    store, cache_root = make_limited_store(tmp_path, size=1000)
    store.create()
    try:
        with store:
            index = store._cache_indexes["data/"]
            original_list = store.cache_backend.list

            def failing_list(backend_name):
                if backend_name.count("/") == 2:  # the scan fails after it has listed 2 directories
                    raise RuntimeError("boom")
                yield from original_list(backend_name)

            store.cache_backend.list = failing_list
            try:
                for i in range(6):
                    store.store(data_name(i), bytes([i]) * 100)  # the 4th item starts a scan
                    assert store.load(data_name(i)) == bytes([i]) * 100
                assert store.stats["cache_errors"] == 1  # the failed scan is not tried again for each item
                assert index.scan is None
                assert index.total == cache_usage(cache_root) == 600
            finally:
                store.cache_backend.list = original_list
    finally:
        store.destroy()
