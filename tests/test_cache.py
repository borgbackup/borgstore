"""Tests for Store optional cache behavior."""

import pytest
import borgstore.store as store_module

from borgstore.backends.errors import ObjectNotFound
from borgstore.constants import DEL_SUFFIX
from borgstore.store import CacheMode, CachePolicy, Store

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
    store, _ = make_store(
        tmp_path, cache={"data/": {"mode": "writethrough"}, "meta/": {"mode": "MIRROR"}, "config/": {"mode": "off"}}
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
        make_store(tmp_path, cache={"data/": "on"})


def test_cache_policy_dict_and_max_age_validation(tmp_path):
    store, _ = make_store(
        tmp_path,
        cache={
            "data/": {"mode": "writethrough", "max_age": 60, "size": 1024},
            "meta/": {"mode": "mirror"},
            "config/": {"mode": "off", "max_age": 0, "size": 0},
        },
    )
    store.create()
    try:
        with store:
            name, value = "data/00000000", b"abc"
            store.store(name, value)
            store._cache_invalidate(store.find(name))
            assert store.load(name) == value
            assert store.load(name) == value
            stats = store.stats
            assert stats["cache_misses"] == 1
            assert stats["cache_hits"] == 1
    finally:
        store.destroy()

    with pytest.raises(ValueError):
        make_store(tmp_path, cache={"data/": {"max_age": 1}})
    with pytest.raises(ValueError):
        make_store(tmp_path, cache={"data/": {"mode": "writethrough", "max_age": -1}})
    with pytest.raises(ValueError):
        make_store(tmp_path, cache={"data/": {"mode": "writethrough", "max_age": "1"}})
    with pytest.raises(ValueError):
        make_store(tmp_path, cache={"data/": {"mode": "writethrough", "size": -1}})
    with pytest.raises(ValueError):
        make_store(tmp_path, cache={"data/": {"mode": "writethrough", "size": 1.5}})
    with pytest.raises(ValueError):
        make_store(tmp_path, cache={"data/": {"mode": "writethrough", "size": "1"}})
    with pytest.raises(ValueError):
        make_store(tmp_path, cache={"data/": {"mode": "writethrough", "unexpected": 1}})
    with pytest.raises(ValueError):
        make_store(tmp_path, cache={"data/": "cache"})
    with pytest.raises(ValueError):
        make_store(tmp_path, cache={"data/": CacheMode.C_WRITETHROUGH})


def test_cache_policy_namedtuple_input(tmp_path):
    store, _ = make_store(tmp_path, cache={"data/": CachePolicy(mode=CacheMode.C_WRITETHROUGH, max_age=60.0, size=512)})
    store.create()
    try:
        with store:
            name, value = "data/00000000", b"abc"
            store.store(name, value)
            store._cache_invalidate(store.find(name))
            assert store.load(name) == value
            assert store.load(name) == value
            stats = store.stats
            assert stats["cache_misses"] == 1
            assert stats["cache_hits"] == 1
    finally:
        store.destroy()


def test_cache_misconfiguration(tmp_path):
    primary_url = (tmp_path / "primary").resolve().as_uri()
    cache_url = (tmp_path / "cache").resolve().as_uri()
    with pytest.raises(ValueError):
        make_store(tmp_path, cache={"data/": {"mode": "writethrough"}}, with_cache_backend=False)
    with pytest.raises(ValueError):
        Store(url=primary_url, levels=LEVELS, cache={"missing/": {"mode": "writethrough"}}, cache_url=cache_url)


def test_cache_off_only_without_backend_is_ok(tmp_path):
    store, _ = make_store(tmp_path, cache={"data/": {"mode": "off"}}, with_cache_backend=False)
    store.create()
    try:
        with store:
            name, value = "data/00000000", b"abc"
            store.store(name, value)
            assert store.load(name) == value
    finally:
        store.destroy()


def test_c_cache_read_through_and_partial_load(tmp_path):
    store, _ = make_store(tmp_path, cache={"data/": {"mode": CacheMode.C_WRITETHROUGH}})
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
    store, _ = make_store(tmp_path, cache={"data/": {"mode": CacheMode.C_MIRROR}})
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


@pytest.mark.parametrize("mode", [CacheMode.C_WRITETHROUGH, CacheMode.C_MIRROR])
def test_write_delete_and_soft_delete_mirror_cache_entries(tmp_path, mode):
    store, _ = make_store(tmp_path, cache={"data/": {"mode": mode}})
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
    store = Store(
        url=primary_url, levels=levels, cache={"data/": {"mode": CacheMode.C_WRITETHROUGH}}, cache_url=cache_url
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
    store, _ = make_store(tmp_path, cache={"data/": {"mode": CacheMode.C_WRITETHROUGH}})
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


def test_c_cache_respects_max_age_since_last_use(tmp_path, monkeypatch):
    store, _ = make_store(tmp_path, cache={"data/": {"mode": CacheMode.C_WRITETHROUGH, "max_age": 5}})
    store.create()
    try:
        with store:
            name, value = "data/00000000", b"abc"
            store.store(name, value)
            nested_name = store.find(name)
            store._cache_invalidate(nested_name)

            now = 1000.0
            atime = 0.0

            def fake_time():
                return now

            monkeypatch.setattr("borgstore.store.time.time", fake_time)
            original_info = store.cache_backend.info
            cache_deletes = {"count": 0}
            original_cache_delete = store.cache_backend.delete

            def wrapped_info(backend_name):
                info = original_info(backend_name)
                if backend_name == nested_name and info.exists:
                    return info._replace(atime=atime)
                return info

            store.cache_backend.info = wrapped_info

            def wrapped_cache_delete(backend_name):
                if backend_name == nested_name:
                    cache_deletes["count"] += 1
                return original_cache_delete(backend_name)

            store.cache_backend.delete = wrapped_cache_delete

            calls = {"load": 0}
            original_load = store.backend.load

            def wrapped(backend_name, size=None, offset=0):
                calls["load"] += 1
                return original_load(backend_name, size=size, offset=offset)

            store.backend.load = wrapped
            try:
                assert store.load(name) == value  # miss, populate cache at t=1000
                atime = 1000.0
                now = 1004.0
                assert store.load(name) == value  # hit, refresh last-used to t=1004
                atime = 1004.0
                now = 1010.0
                assert store.load(name) == value  # expired, miss again
            finally:
                store.backend.load = original_load
                store.cache_backend.info = original_info
                store.cache_backend.delete = original_cache_delete

            assert calls["load"] == 2
            assert cache_deletes["count"] == 2
            stats = store.stats
            assert stats["cache_misses"] == 2
            assert stats["cache_hits"] == 1
    finally:
        store.destroy()


def test_cache_errors_do_not_fail_main_operations(tmp_path):
    store, _ = make_store(tmp_path, cache={"data/": {"mode": CacheMode.C_WRITETHROUGH}})
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
        tmp_path, cache={"data/": {"mode": CacheMode.C_WRITETHROUGH}, "meta/": {"mode": CacheMode.C_MIRROR}}
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
            assert stats["cache_bytes_read"] == 6
            assert stats["cache_bytes_written"] == 9
            assert stats["cache_disabled"] is False
            assert stats["cache_hit_ratio"] == 1.0
    finally:
        store.destroy()


def test_close_cleans_up_expired_cache_items(tmp_path, monkeypatch):
    store, _ = make_store(tmp_path, cache={"data/": {"mode": CacheMode.C_WRITETHROUGH, "max_age": 5}})
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
    delete_calls = {"count": 0}
    original_delete = store.cache_backend.delete

    def wrapped_list(backend_name):
        for info in original_list(backend_name):
            full_name = (backend_name + "/" + info.name) if backend_name else info.name
            if full_name == nested_name and info.exists:
                yield info._replace(atime=atime)
            else:
                yield info

    store.cache_backend.list = wrapped_list

    def wrapped_delete(backend_name):
        if backend_name == nested_name:
            delete_calls["count"] += 1
        return original_delete(backend_name)

    store.cache_backend.delete = wrapped_delete
    try:
        store.open()
        store.close()
        assert delete_calls["count"] == 1

        atime = 1999.0
        store.open()
        store.store(name, value)
        store.close()
        assert delete_calls["count"] == 1
    finally:
        store.cache_backend.list = original_list
        store.cache_backend.delete = original_delete
        store.destroy()


def test_close_cleans_up_lru_cache_items_by_size(tmp_path, monkeypatch):
    store, _ = make_store(tmp_path, cache={"data/": {"mode": CacheMode.C_WRITETHROUGH, "size": 7}})
    store.create()
    names_values = [("data/00000000", b"aaaa"), ("data/00000001", b"bbbb"), ("data/00000002", b"cccc")]
    store.open()
    for name, value in names_values:
        store.store(name, value)
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
    store, _ = make_store(tmp_path, cache={"data/": {"mode": CacheMode.C_WRITETHROUGH, "max_age": 50, "size": 7}})
    store.create()
    names_values = [("data/00000000", b"aaaa"), ("data/00000001", b"bbbb"), ("data/00000002", b"cccc")]
    store.open()
    for name, value in names_values:
        store.store(name, value)
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


def test_close_cleanup_errors_are_best_effort(tmp_path):
    store, _ = make_store(tmp_path, cache={"data/": {"mode": CacheMode.C_WRITETHROUGH, "max_age": 5}})
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
    store, _ = make_store(tmp_path, cache={"data/": {"mode": CacheMode.C_WRITETHROUGH}})
    store.create()
    try:
        with store:
            name, value = "data/00000000", b"abc"
            store.store(name, value)

            nested = store.find(name)
            store._cache_invalidate(nested)

            original_primary_load = store.backend.load
            primary_calls = {"count": 0}

            def wrapped_primary_load(backend_name, size=None, offset=0):
                primary_calls["count"] += 1
                return original_primary_load(backend_name, size=size, offset=offset)

            sleep_calls = []
            original_sleep = store_module.time.sleep

            def wrapped_sleep(seconds):
                sleep_calls.append(seconds)

            monkeypatch.setattr("borgstore.store.time.sleep", wrapped_sleep)
            store.backend.load = wrapped_primary_load
            try:
                assert store.load(name) == value
                assert primary_calls["count"] == 1
                sleeps_after_miss = len(sleep_calls)
                assert sleeps_after_miss >= 1

                assert store.load(name) == value
                assert primary_calls["count"] == 1
                # cache hit: no primary backend calls at all, so no new sleeps
                assert len(sleep_calls) == sleeps_after_miss
            finally:
                store.backend.load = original_primary_load
                monkeypatch.setattr("borgstore.store.time.sleep", original_sleep)
    finally:
        store.destroy()


def test_bandwidth_emulation_not_applied_to_cache_backend_calls(tmp_path, monkeypatch):
    monkeypatch.setenv("BORGSTORE_LATENCY", "0")
    monkeypatch.setenv("BORGSTORE_BANDWIDTH", "8")  # 1 byte/s
    store, _ = make_store(tmp_path, cache={"data/": {"mode": CacheMode.C_WRITETHROUGH}})
    store.create()
    try:
        with store:
            name, value = "data/00000000", b"abc"
            store.store(name, value)
            nested = store.find(name)
            store._cache_invalidate(nested)

            primary_calls = {"count": 0}
            original_primary_load = store.backend.load

            def wrapped_primary_load(backend_name, size=None, offset=0):
                primary_calls["count"] += 1
                return original_primary_load(backend_name, size=size, offset=offset)

            sleep_calls = []
            original_sleep = store_module.time.sleep

            def wrapped_sleep(seconds):
                sleep_calls.append(seconds)

            monkeypatch.setattr("borgstore.store.time.sleep", wrapped_sleep)
            store.backend.load = wrapped_primary_load
            try:
                assert store.load(name) == value
                assert primary_calls["count"] == 1
                sleeps_after_miss = len(sleep_calls)
                assert any(seconds >= 2.9 for seconds in sleep_calls)

                assert store.load(name) == value
                assert primary_calls["count"] == 1
                assert len(sleep_calls) == sleeps_after_miss
            finally:
                store.backend.load = original_primary_load
                monkeypatch.setattr("borgstore.store.time.sleep", original_sleep)
    finally:
        store.destroy()
