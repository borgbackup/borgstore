"""Tests for PosixFS quota support."""

import time

import pytest

from borgstore.backends.posixfs import PosixFS
from borgstore.backends.errors import QuotaExceeded
from borgstore.constants import QUOTA_STORE_NAME, QUOTA_PERSIST_DELTA, QUOTA_PERSIST_INTERVAL


@pytest.fixture()
def backend(tmp_path):
    """Create a PosixFS backend with quota enabled."""
    be = PosixFS(tmp_path / "store", quota=1000)
    be.create()
    try:
        yield be
    finally:
        be.destroy()


@pytest.fixture()
def backend_no_quota(tmp_path):
    """Create a PosixFS backend without a quota."""
    be = PosixFS(tmp_path / "store")
    be.create()
    try:
        yield be
    finally:
        be.destroy()


class TestQuotaTracking:
    def test_store_and_delete_track_size(self, backend):
        """Storing/Deleting an object increases/decreases tracked quota usage."""
        backend.open()
        data = b"x" * 100
        backend.store("testobj", data)
        assert backend._quota_use == 100
        backend.delete("testobj")
        assert backend._quota_use == 0
        backend.close()

    def test_overwrite_tracks_delta(self, backend):
        """Overwriting an object tracks the size delta correctly."""
        backend.open()
        backend.store("testobj", b"x" * 100)
        assert backend._quota_use == 100
        backend.store("testobj", b"x" * 250)
        assert backend._quota_use == 250
        backend.store("testobj", b"x" * 50)
        assert backend._quota_use == 50
        backend.close()

    def test_multiple_objects(self, backend):
        """Quota tracks the cumulative size of multiple objects."""
        backend.open()
        backend.store("obj1", b"a" * 100)
        backend.store("obj2", b"b" * 200)
        backend.store("obj3", b"c" * 300)
        assert backend._quota_use == 600
        backend.delete("obj2")
        assert backend._quota_use == 400
        backend.close()


class TestQuotaEnforcement:
    def test_store_within_quota(self, backend):
        """Storing within a quota succeeds."""
        backend.open()
        backend.store("testobj", b"x" * 1000)
        assert backend._quota_use == 1000
        backend.close()

    def test_store_exceeds_quota(self, backend):
        """Storing beyond quota raises QuotaExceeded."""
        backend.open()
        with pytest.raises(QuotaExceeded):
            backend.store("testobj", b"x" * 1001)
        # quota usage should not have changed
        assert backend._quota_use == 0
        backend.close()

    def test_cumulative_exceeds_quota(self, backend):
        """Cumulative stores exceeding quota raises QuotaExceeded."""
        backend.open()
        backend.store("obj1", b"x" * 600)
        with pytest.raises(QuotaExceeded):
            backend.store("obj2", b"x" * 500)
        assert backend._quota_use == 600
        backend.close()

    def test_overwrite_within_quota(self, backend):
        """Overwriting with a smaller value keeps within quota."""
        backend.open()
        backend.store("testobj", b"x" * 900)
        # overwrite with larger value that would exceed quota
        with pytest.raises(QuotaExceeded):
            backend.store("testobj", b"x" * 1001)
        # original value should still be tracked
        assert backend._quota_use == 900
        backend.close()

    def test_overwrite_shrink_allows_new_store(self, backend):
        """Shrinking an object frees quota for new stores."""
        backend.open()
        backend.store("obj1", b"x" * 800)
        # overwrite to shrink
        backend.store("obj1", b"x" * 200)
        assert backend._quota_use == 200
        # now we have room for more
        backend.store("obj2", b"x" * 800)
        assert backend._quota_use == 1000
        backend.close()

    def test_no_quota_unlimited(self, backend_no_quota):
        """Without a quota set, stores are unlimited and no quota tracking happens."""
        backend_no_quota.open()
        backend_no_quota.store("testobj", b"x" * 100000)
        assert backend_no_quota._quota_use == 0  # no tracking when quota is None
        backend_no_quota.close()


class TestQuotaPersistence:
    def test_quota_persisted_on_close(self, backend):
        """Quota usage is persisted to disk on close."""
        backend.open()
        backend.store("testobj", b"x" * 100)
        backend.close()
        # re-open and verify persisted value
        backend.open()
        assert backend._quota_use == 100
        backend.close()

    def test_quota_survives_reopen(self, backend):
        """Quota usage survives close/open cycles."""
        backend.open()
        backend.store("obj1", b"x" * 100)
        backend.store("obj2", b"x" * 200)
        backend.close()

        backend.open()
        assert backend._quota_use == 300
        backend.delete("obj1")
        backend.close()

        backend.open()
        assert backend._quota_use == 200
        backend.close()

    def test_persist_threshold_delta(self, backend):
        """Quota is not persisted to disk for small changes (below QUOTA_PERSIST_DELTA)."""
        backend.open()
        # store a small object (below 10MB threshold)
        backend.store("testobj", b"x" * 100)
        # the in-memory value is updated
        assert backend._quota_use == 100
        # but the persisted value should NOT have been updated (delta < 10MB)
        assert backend._quota_use_persisted == 0
        # the on-disk value should still be 0
        assert int(backend._quota_path().read_text()) == 0
        backend.close()
        # after close, force-persist writes it
        assert int(backend._quota_path().read_text()) == 100

    def test_persist_threshold_time(self, backend):
        """Quota is persisted when enough time has elapsed."""
        backend.open()
        backend.store("testobj", b"x" * 100)
        assert backend._quota_use_persisted == 0  # not persisted yet (small delta)

        # simulate time passing beyond the interval
        backend._quota_last_persist_time = time.monotonic() - QUOTA_PERSIST_INTERVAL - 1
        backend.store("testobj2", b"x" * 50)
        # now it should have been persisted due to time
        assert backend._quota_use_persisted == 150
        assert int(backend._quota_path().read_text()) == 150
        backend.close()

    def test_persist_threshold_large_delta(self, tmp_path):
        """Quota is persisted immediately when delta exceeds QUOTA_PERSIST_DELTA."""
        be = PosixFS(tmp_path / "store", quota=QUOTA_PERSIST_DELTA * 2)
        be.create()
        be.open()
        # store a large object exceeding the delta threshold
        be.store("bigobj", b"x" * (QUOTA_PERSIST_DELTA + 1))
        assert be._quota_use_persisted == QUOTA_PERSIST_DELTA + 1
        assert int(be._quota_path().read_text()) == QUOTA_PERSIST_DELTA + 1
        be.close()
        be.destroy()


class TestHiddenFiles:
    def test_hid_files_not_in_list(self, backend):
        """Hidden .hid files are not visible in list() results."""
        backend.open()
        backend.store("visible", b"data")
        names = [info.name for info in backend.list("")]
        assert "visible" in names
        assert QUOTA_STORE_NAME not in names
        backend.close()

    def test_cannot_store_hid_name(self, backend):
        """Users cannot store objects with .hid suffix."""
        backend.open()
        with pytest.raises(ValueError, match="must not end with .hid"):
            backend.store("secret.hid", b"data")
        backend.close()

    def test_cannot_load_hid_name(self, backend):
        """Users cannot load objects with .hid suffix."""
        backend.open()
        with pytest.raises(ValueError, match="must not end with .hid"):
            backend.load("quota.hid")
        backend.close()

    def test_cannot_delete_hid_name(self, backend):
        """Users cannot delete objects with .hid suffix."""
        backend.open()
        with pytest.raises(ValueError, match="must not end with .hid"):
            backend.delete("quota.hid")
        backend.close()

    def test_cannot_info_hid_name(self, backend):
        """Users cannot get info on objects with .hid suffix."""
        backend.open()
        with pytest.raises(ValueError, match="must not end with .hid"):
            backend.info("quota.hid")
        backend.close()


class TestQuotaSentinel:
    def test_no_quota_no_quota_file_on_create(self, tmp_path):
        """When quota is None, create() does not create a quota file."""
        be = PosixFS(tmp_path / "store")
        be.create()
        quota_path = be.base_path / QUOTA_STORE_NAME
        assert not quota_path.exists()
        be.destroy()

    def test_no_quota_no_quota_file_on_close(self, backend_no_quota):
        """When quota is None, close() does not create a quota file."""
        backend_no_quota.open()
        backend_no_quota.store("testobj", b"x" * 100)
        backend_no_quota.close()
        quota_path = backend_no_quota.base_path / QUOTA_STORE_NAME
        assert not quota_path.exists()

    def test_no_quota_no_quota_file_on_open(self, backend_no_quota):
        """When quota is None, open() does not create a quota file."""
        backend_no_quota.open()
        quota_path = backend_no_quota.base_path / QUOTA_STORE_NAME
        assert not quota_path.exists()
        backend_no_quota.close()

    def test_quota_scan_on_no_quota_file(self, tmp_path):
        """When a quota is set and no quota file exists, open() scans the filesystem."""
        # First, create without quota (no quota file)
        be = PosixFS(tmp_path / "store")
        be.create()
        be.open()
        be.store("obj1", b"x" * 100)
        be.store("obj2", b"x" * 200)
        be.close()
        # Now reopen with quota enabled - should scan and find 300 bytes
        be2 = PosixFS(tmp_path / "store", quota=10000)
        be2.open()
        assert be2._quota_use == 300
        be2.close()

    def test_transition_tracked_to_untracked(self, tmp_path):
        """Transitioning from quota tracked to untracked deletes the quota file."""
        # Create and use with quota enabled
        be = PosixFS(tmp_path / "store", quota=10000)
        be.create()
        be.open()
        be.store("obj1", b"x" * 100)
        be.store("obj2", b"x" * 200)
        be.close()
        quota_path = be.base_path / QUOTA_STORE_NAME
        assert int(quota_path.read_text()) == 300
        # Reopen without quota - should delete quota file
        be2 = PosixFS(tmp_path / "store")
        be2.open()
        assert not quota_path.exists()
        be2.close()
        assert not quota_path.exists()
        be2.destroy()

    def test_transition_untracked_to_tracked(self, tmp_path):
        """Transitioning from untracked to tracked scans filesystem for usage."""
        # Create without quota, add content
        be = PosixFS(tmp_path / "store")
        be.create()
        be.open()
        be.store("obj1", b"x" * 100)
        be.store("obj2", b"x" * 200)
        be.close()
        quota_path = be.base_path / QUOTA_STORE_NAME
        assert not quota_path.exists()
        # Reopen with quota - should scan and determine usage
        be2 = PosixFS(tmp_path / "store", quota=10000)
        be2.open()
        assert be2._quota_use == 300
        be2.close()
        assert int(quota_path.read_text()) == 300
        be2.destroy()

    def test_transition_roundtrip(self, tmp_path):
        """Full round-trip: tracked → untracked → tracked preserves correct usage."""
        store_path = tmp_path / "store"
        # Phase 1: create with quota, store data
        be = PosixFS(store_path, quota=10000)
        be.create()
        be.open()
        be.store("obj1", b"x" * 100)
        be.close()
        # Phase 2: reopen without quota, add more data
        be2 = PosixFS(store_path)
        be2.open()
        be2.store("obj2", b"x" * 200)
        be2.close()
        quota_path = be2.base_path / QUOTA_STORE_NAME
        assert not quota_path.exists()
        # Phase 3: reopen with quota - should scan and find all 300 bytes
        be3 = PosixFS(store_path, quota=10000)
        be3.open()
        assert be3._quota_use == 300
        # Add more and verify tracking continues correctly
        be3.store("obj3", b"x" * 50)
        assert be3._quota_use == 350
        be3.close()
        assert int(quota_path.read_text()) == 350
        be3.destroy()

    def test_transition_untracked_to_tracked_enforces_quota(self, tmp_path):
        """After transitioning to tracked, a quota is enforced against scanned usage."""
        # Create without quota, add lots of content
        be = PosixFS(tmp_path / "store")
        be.create()
        be.open()
        be.store("obj1", b"x" * 800)
        be.close()
        # Reopen with a tight quota - scanned usage should be enforced
        be2 = PosixFS(tmp_path / "store", quota=1000)
        be2.open()
        assert be2._quota_use == 800
        with pytest.raises(QuotaExceeded):
            be2.store("obj2", b"x" * 300)
        be2.close()
        be2.destroy()

    def test_quota_scan_with_subdirs(self, tmp_path):
        """Filesystem scan includes files in subdirectories."""
        be = PosixFS(tmp_path / "store")
        be.create()
        be.open()
        be.mkdir("sub")
        be.store("sub/obj1", b"x" * 150)
        be.store("toplevel", b"x" * 50)
        be.close()
        # Reopen with quota - should scan and find all files
        be2 = PosixFS(tmp_path / "store", quota=10000)
        be2.open()
        assert be2._quota_use == 200
        be2.close()


class TestQuotaConcurrency:
    def test_concurrent_sessions_preserve_updates(self, tmp_path):
        """Two concurrent sessions both contribute to the on-disk quota value."""
        store_path = tmp_path / "store"
        quota = 100000
        be = PosixFS(store_path, quota=quota)
        be.create()

        # Open two sessions on the same store
        session_a = PosixFS(store_path, quota=quota)
        session_b = PosixFS(store_path, quota=quota)
        session_a.open()
        session_b.open()

        # Both start at 0
        assert session_a._quota_use == 0
        assert session_b._quota_use == 0

        # Session A stores 500 bytes and closes (force-persists)
        session_a.store("obj_a", b"a" * 500)
        session_a.close()

        # Session B stores 300 bytes and closes (force-persists)
        session_b.store("obj_b", b"b" * 300)
        session_b.close()

        # On-disk value should reflect both sessions' contributions
        quota_path = store_path / QUOTA_STORE_NAME
        on_disk = int(quota_path.read_text())
        assert on_disk == 800  # 500 + 300, not just 300

        # Reopen and verify
        session_c = PosixFS(store_path, quota=quota)
        session_c.open()
        assert session_c._quota_use == 800
        session_c.close()

    def test_concurrent_sessions_store_and_delete(self, tmp_path):
        """Concurrent store and delete across sessions track correctly."""
        store_path = tmp_path / "store"
        quota = 100000
        be = PosixFS(store_path, quota=quota)
        be.create()

        # Session A stores data
        session_a = PosixFS(store_path, quota=quota)
        session_a.open()
        session_a.store("obj1", b"x" * 1000)
        session_a.close()

        # Two sessions open concurrently, both see 1000
        session_b = PosixFS(store_path, quota=quota)
        session_c = PosixFS(store_path, quota=quota)
        session_b.open()
        session_c.open()
        assert session_b._quota_use == 1000
        assert session_c._quota_use == 1000

        # Session B adds 500
        session_b.store("obj2", b"y" * 500)
        session_b.close()

        # Session C deletes 1000
        session_c.delete("obj1")
        session_c.close()

        # Net result: 1000 + 500 - 1000 = 500
        quota_path = store_path / QUOTA_STORE_NAME
        on_disk = int(quota_path.read_text())
        assert on_disk == 500

    def test_concurrent_interleaved_persists(self, tmp_path):
        """Interleaved persists from two sessions accumulate correctly."""
        store_path = tmp_path / "store"
        quota = 100000
        be = PosixFS(store_path, quota=quota)
        be.create()

        session_a = PosixFS(store_path, quota=quota)
        session_b = PosixFS(store_path, quota=quota)
        session_a.open()
        session_b.open()

        # Session A stores and force-persists
        session_a.store("obj_a1", b"a" * 200)
        session_a._quota_update(0, force=True)

        # Session B stores and force-persists
        session_b.store("obj_b1", b"b" * 300)
        session_b._quota_update(0, force=True)

        # Session A stores more and force-persists
        session_a.store("obj_a2", b"a" * 100)
        session_a._quota_update(0, force=True)

        session_a.close()
        session_b.close()

        # All contributions should be reflected
        quota_path = store_path / QUOTA_STORE_NAME
        on_disk = int(quota_path.read_text())
        assert on_disk == 600  # 200 + 300 + 100
