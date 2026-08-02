"""
Tests for Store thread safety (#206): all operations of a Store shared between threads
are serialized by an internal lock, so the (not thread-safe) backend never sees
concurrent calls.  list() stays lazy: the lock is only held per fetched item.
"""

import threading
import time

import pytest

from . import key
from borgstore.backends.posixfs import PosixFS
from borgstore.store import Store

CONFIG = {"zero/": {"levels": [0]}}


class SerializationAssertingBackend:
    """Wraps a backend; every call checks that no other backend call is in progress.

    The Store's lock must make concurrent backend calls impossible; without it, the small
    sleep inside each call makes overlapping calls from multiple threads very likely.
    """

    def __init__(self, backend):
        self._backend = backend
        self._busy = False
        self.violations = 0
        self.calls = 0

    def _enter(self):
        if self._busy:
            self.violations += 1
        self._busy = True
        self.calls += 1
        time.sleep(0.0005)  # widen the race window, so unserialized calls actually overlap

    def _exit(self):
        self._busy = False

    def __getattr__(self, name):
        attr = getattr(self._backend, name)
        if not callable(attr):
            return attr  # e.g. precreate_dirs

        if name == "list":
            # the backend list generator runs stepwise; guard each step, not the whole iteration.
            def guarded_list(*args, **kwargs):
                inner = attr(*args, **kwargs)
                while True:
                    self._enter()
                    try:
                        info = next(inner)
                    except StopIteration:
                        break
                    finally:
                        self._exit()
                    yield info

            return guarded_list

        def guarded(*args, **kwargs):
            self._enter()
            try:
                return attr(*args, **kwargs)
            finally:
                self._exit()

        return guarded


@pytest.fixture()
def asserting_store(tmp_path):
    backend = SerializationAssertingBackend(PosixFS(tmp_path / "store"))
    store = Store(backend=backend, config=CONFIG)
    store.create()
    with store:
        yield store, backend
    store.destroy()


def test_concurrent_ops_are_serialized(asserting_store):
    store, backend = asserting_store
    errors = []

    def hammer(thread_no):
        try:
            for i in range(20):
                k = f"zero/{key(thread_no * 1000 + i)}"
                value = f"{thread_no}-{i}".encode()
                store.store(k, value)
                assert store.load(k) == value
                store.info(k)
                if i % 5 == 0:
                    list(store.list("zero"))
                store.delete(k)
        except Exception as exc:  # raising in a thread would go unnoticed by pytest
            errors.append(exc)

    threads = [threading.Thread(target=hammer, args=(n,)) for n in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert backend.calls > 0
    assert backend.violations == 0  # no backend call overlapped another one


def test_list_is_lazy_and_interleaves(asserting_store):
    store, backend = asserting_store
    for i in range(50):
        store.store(f"zero/{key(i)}", b"value")

    # the iterating thread itself can do store operations inside its listing loop
    # (the lock is not held while the generator is suspended):
    seen = 0
    for info in store.list("zero"):
        seen += 1
        store.info(f"zero/{info.name}")
    assert seen == 50

    # ... and another thread's operations interleave with a long listing:
    listing_started = threading.Event()
    other_done = []

    def other_thread():
        listing_started.wait()
        for i in range(10):
            k = f"zero/{key(100 + i)}"
            store.store(k, b"other")
            store.delete(k)
        other_done.append(True)

    t = threading.Thread(target=other_thread)
    t.start()
    seen = 0
    for _info in store.list("zero"):
        seen += 1
        if seen == 1:
            listing_started.set()
        time.sleep(0.001)  # keep the listing running while the other thread works
    t.join()
    assert seen >= 50  # the other thread's short-lived items may or may not be seen
    assert other_done == [True]
    assert backend.violations == 0


def test_stats_safe_under_concurrency(asserting_store):
    store, backend = asserting_store
    n_threads, n_ops = 4, 25

    def hammer(thread_no):
        for i in range(n_ops):
            k = f"zero/{key(thread_no * 1000 + i)}"
            store.store(k, b"x")
            store.load(k)

    threads = [threading.Thread(target=hammer, args=(n,)) for n in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stats = store.stats
    # without serialization, the lost-update races on the Counter would lose increments.
    assert stats["store_calls"] == n_threads * n_ops
    assert stats["load_calls"] == n_threads * n_ops
    assert backend.violations == 0
