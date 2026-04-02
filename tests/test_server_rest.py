import hashlib
import threading
import pytest

try:
    import requests
except ImportError:
    pytest.skip("requests is not installed", allow_module_level=True)

from borgstore.constants import DEL_SUFFIX
from borgstore.server.rest import BorgStoreRESTServer
from borgstore.backends.rest import get_rest_backend
from borgstore.backends.posixfs import get_file_backend
from borgstore.backends.errors import ObjectNotFound, BackendAlreadyExists, QuotaExceeded


def start_server(backend_url, address, port, username=None, password=None, permissions=None, quota=None):
    from borgstore.store import get_backend

    backend = get_backend(backend_url, permissions=permissions, quota=quota)
    server = BorgStoreRESTServer((address, port), backend, username, password)
    ready = threading.Event()

    def _serve():
        ready.set()
        server.serve_forever()

    thread = threading.Thread(target=_serve)
    thread.daemon = True
    thread.start()
    ready.wait(timeout=5.0)
    return server, thread


@pytest.fixture
def rest_server_with_auth(tmp_path):
    backend_url = tmp_path.as_uri()
    address, port = "127.0.0.1", 0
    username, password = "testuser", "testpassword"

    server, thread = start_server(backend_url, address, port, username, password)

    host, assigned_port = server.server_address
    backend = get_rest_backend(f"http://{username}:{password}@{host}:{assigned_port}/")
    yield backend

    server.shutdown()
    server.server_close()


def test_rest_server_basic_ops(rest_server_with_auth):
    be = rest_server_with_auth
    be.create()
    be.open()
    try:
        be.store("test/item1", b"hello world")
        assert be.load("test/item1") == b"hello world"

        info = be.info("test/item1")
        assert info.exists
        assert info.size == 11

        assert be.load("test/item1", offset=6) == b"world"
        assert be.load("test/item1", size=5) == b"hello"
        assert be.load("test/item1", offset=6, size=2) == b"wo"

        items = list(be.list("test"))
        assert len(items) == 1
        assert items[0].name == "item1"
        assert items[0].size == 11

        be.delete("test/item1")
        with pytest.raises(ObjectNotFound):
            be.load("test/item1")
        assert not be.info("test/item1").exists

    finally:
        be.close()
        try:
            be.destroy()
        except Exception:
            pass


def test_rest_server_invalid_accept(tmp_path):
    backend_url = tmp_path.as_uri()
    address, port = "127.0.0.1", 0
    server, thread = start_server(backend_url, address, port)

    host, assigned_port = server.server_address
    try:
        # Manually send a request without the required Accept header
        url = f"http://{host}:{assigned_port}/"
        response = requests.post(url + "?cmd=create")
        assert response.status_code == 406
        assert "Not Acceptable" in response.text

        # Send with wrong Accept header
        response = requests.post(url + "?cmd=create", headers={"Accept": "text/plain"})
        assert response.status_code == 406
    finally:
        server.shutdown()
        server.server_close()


def test_rest_server_error_mapping(rest_server_with_auth):
    be = rest_server_with_auth
    be.create()
    be.open()
    try:
        be.store("test/item1", b"data")
    finally:
        be.close()

    # Test BackendAlreadyExists (409) - directory is now non-empty
    with pytest.raises(BackendAlreadyExists):
        be.create()

    be.open()
    try:
        # Test ObjectNotFound (404)
        with pytest.raises(ObjectNotFound):
            be.load("nonexistent")

        # Test ValueError (400) - invalid name
        with pytest.raises(ValueError):
            be.store("../invalid", b"data")

    finally:
        be.close()


def test_rest_server_move(rest_server_with_auth):
    be = rest_server_with_auth
    be.create()
    be.open()
    try:
        data = b"move me"
        be.store("test/item", data)
        assert be.load("test/item") == data

        be.move("test/item", "test/item" + DEL_SUFFIX)
        assert not be.info("test/item").exists
        assert be.info("test/item" + DEL_SUFFIX).exists

        be.move("test/item" + DEL_SUFFIX, "test/item")
        assert be.load("test/item") == data
        assert not be.info("test/item" + DEL_SUFFIX).exists

    finally:
        be.close()


def test_rest_server_permissions(tmp_path):
    from borgstore.backends.errors import PermissionDenied

    backend_path = tmp_path / "backend"
    backend_url = backend_path.as_uri()

    # We need to create the directory and a file first, using a file backend with
    # full permissions. Later this path will be restricted to read-only.
    setup_be = get_file_backend(backend_url)
    setup_be.create()
    with setup_be:
        setup_be.store("readonly/ro_item", b"ro_data")

    address, port = "127.0.0.1", 0
    # Grant only read permission for "readonly" and full permissions for everything else
    permissions = {"": "lrwWD", "readonly": "lr"}
    server, thread = start_server(backend_url, address, port, permissions=permissions)

    host, assigned_port = server.server_address
    be = get_rest_backend(f"http://{host}:{assigned_port}/")
    try:
        with be:
            # Full permissions at root/other paths
            be.store("item1", b"data1")
            assert be.load("item1") == b"data1"

            # Try to store in readonly - should fail
            with pytest.raises(PermissionDenied):
                be.store("readonly/item2", b"data2")

            # Try to delete in readonly - should fail
            with pytest.raises(PermissionDenied):
                be.delete("readonly/ro_item")

            # Verify load and list work in readonly
            assert be.load("readonly/ro_item") == b"ro_data"
            items = list(be.list("readonly"))
            assert len(items) == 1
            assert items[0].name == "ro_item"
    finally:
        server.shutdown()
        server.server_close()


def test_rest_server_http11(tmp_path):
    backend_url = tmp_path.as_uri()
    address, port = "127.0.0.1", 0
    server, thread = start_server(backend_url, address, port)

    host, assigned_port = server.server_address
    url = f"http://{host}:{assigned_port}/"
    headers = {"Accept": "application/vnd.x.borgstore.rest.v1"}

    try:
        with requests.Session() as s:
            # Check protocol version
            response = s.post(url + "?cmd=create", headers=headers)
            assert response.status_code == 200
            # requests.Response doesn't directly expose the protocol version string like "HTTP/1.1"
            # but we can check the underlying urllib3 response or better, check if it's 11.
            assert response.raw.version == 11  # 11 means HTTP/1.1 in urllib3

            # Perform another request in the same session to verify persistence
            response = s.get(url + "testitem", headers=headers)
            assert response.status_code == 404  # Not found is expected, but request should succeed
            assert response.raw.version == 11
    finally:
        server.shutdown()
        server.server_close()


def test_rest_server_auth_required(tmp_path):
    """Test that authentication is required and that bad credentials are rejected."""
    backend_url = tmp_path.as_uri()
    server, thread = start_server(backend_url, "127.0.0.1", 0, username="user", password="secret")

    host, assigned_port = server.server_address
    url = f"http://{host}:{assigned_port}/"
    headers = {"Accept": "application/vnd.x.borgstore.rest.v1"}
    try:
        # No credentials at all → 401
        response = requests.post(url + "?cmd=create", headers=headers)
        assert response.status_code == 401
        assert "WWW-Authenticate" in response.headers

        # Wrong password → 401
        response = requests.post(url + "?cmd=create", headers=headers, auth=("user", "wrongpassword"))
        assert response.status_code == 401

        # Wrong username → 401
        response = requests.post(url + "?cmd=create", headers=headers, auth=("wronguser", "secret"))
        assert response.status_code == 401

        # Correct credentials → 200
        response = requests.post(url + "?cmd=create", headers=headers, auth=("user", "secret"))
        assert response.status_code == 200
    finally:
        server.shutdown()
        server.server_close()


def test_rest_server_hash(rest_server_with_auth):
    be = rest_server_with_auth
    be.create()
    be.open()
    try:
        data = b"hash me"
        expected_hash = hashlib.sha256(data).hexdigest()
        be.store("test/item", data)
        assert be.hash("test/item") == expected_hash
        assert be.hash("test/item", algorithm="sha256") == expected_hash

        # Test unsupported algorithm
        with pytest.raises(ValueError, match="Unsupported hash algorithm"):
            be.hash("test/item", algorithm="invalid_algo")

        # Large-ish data to test chunking (though 2MB isn't very large, it's enough to cross 1MB chunk)
        large_data = b"a" * (2 * 1024 * 1024)
        expected_large_hash = hashlib.sha256(large_data).hexdigest()
        be.store("test/large_item", large_data)
        assert be.hash("test/large_item") == expected_large_hash

        # Test error for nonexistent object
        with pytest.raises(ObjectNotFound):
            be.hash("test/nonexistent")
    finally:
        be.close()


def test_rest_server_defrag(tmp_path):
    import json
    import requests

    backend_url = tmp_path.as_uri()
    address, port = "127.0.0.1", 0
    username, password = "testuser", "testpassword"

    server, thread = start_server(backend_url, address, port, username, password)
    host, assigned_port = server.server_address
    url = f"http://{host}:{assigned_port}/"
    headers = {"Accept": "application/vnd.x.borgstore.rest.v1"}
    auth = (username, password)

    try:
        # 1. Create backend
        requests.post(url + "?cmd=create", auth=auth, headers=headers).raise_for_status()

        # 2. Store some initial data
        requests.post(url + "file1", data=b"0123456789", auth=auth, headers=headers).raise_for_status()
        requests.post(url + "file2", data=b"abcdefghij", auth=auth, headers=headers).raise_for_status()

        # 3. Call defrag
        # We want to take "234" from file1 (offset 2, size 3) and "fg" from file2 (offset 5, size 2)
        # Expected result: "234fg"
        sources = [("file1", 2, 3), ("file2", 5, 2)]
        response = requests.post(
            url + "?cmd=defrag&target=targetfile", data=json.dumps(sources), auth=auth, headers=headers
        )
        response.raise_for_status()
        assert response.text == "targetfile"
        assert response.headers["Content-Type"] == "text/plain"

        # 4. Verify the result
        response = requests.get(url + "targetfile", auth=auth, headers=headers)
        response.raise_for_status()
        assert response.content == b"234fg"

        # 5. Test with empty list
        response = requests.post(url + "?cmd=defrag&target=emptyfile", data=json.dumps([]), auth=auth, headers=headers)
        response.raise_for_status()
        response = requests.get(url + "emptyfile", auth=auth, headers=headers)
        assert response.content == b""

        # 6. Test with missing target
        response = requests.post(url + "?cmd=defrag", data=json.dumps(sources), auth=auth, headers=headers)
        assert response.status_code == 400
        assert "Missing target or algorithm" in response.text

        # 7. Test with algorithm but no target
        combined_data = b"234fg"
        algo = "sha256"
        expected_hash = hashlib.sha256(combined_data).hexdigest()
        response = requests.post(
            url + f"?cmd=defrag&algorithm={algo}", data=json.dumps(sources), auth=auth, headers=headers
        )
        response.raise_for_status()
        assert response.text == expected_hash
        assert response.headers["Content-Type"] == "text/plain"
        response = requests.get(url + expected_hash, auth=auth, headers=headers)
        assert response.content == combined_data

        # 8. Test that target overrides algorithm
        # Even if algorithm is provided, if target is also provided, target is used.
        response = requests.post(
            url + f"?cmd=defrag&target=override_target&algorithm={algo}",
            data=json.dumps(sources),
            auth=auth,
            headers=headers,
        )
        response.raise_for_status()
        assert response.text == "override_target"
        response = requests.get(url + "override_target", auth=auth, headers=headers)
        assert response.content == combined_data

        # 9. Test with levels=1 and algorithm
        from borgstore.utils.nesting import nest

        response = requests.post(
            url + f"?cmd=defrag&algorithm={algo}&levels=1", data=json.dumps(sources), auth=auth, headers=headers
        )
        response.raise_for_status()
        expected_nested_res = nest(expected_hash, levels=1)
        assert response.text == expected_nested_res
        response = requests.get(url + expected_nested_res, auth=auth, headers=headers)
        assert response.content == combined_data

        # 10. Test with namespace, levels=1 and algorithm
        namespace = "ns1"
        response = requests.post(
            url + f"?cmd=defrag&algorithm={algo}&namespace={namespace}&levels=1",
            data=json.dumps(sources),
            auth=auth,
            headers=headers,
        )
        response.raise_for_status()
        expected_nested_res_ns = nest(namespace + "/" + expected_hash, levels=1)
        assert response.text == expected_nested_res_ns
        response = requests.get(url + expected_nested_res_ns, auth=auth, headers=headers)
        assert response.content == combined_data

    finally:
        server.shutdown()
        server.server_close()


def test_rest_backend_defrag(rest_server_with_auth):
    be = rest_server_with_auth
    be.create()
    be.open()
    try:
        be.store("file1", b"0123456789")
        be.store("file2", b"abcdefghij")

        # Test defrag with target
        sources = [("file1", 2, 3), ("file2", 5, 2)]
        res = be.defrag(sources, target="target1")
        assert res == "target1"
        assert be.load("target1") == b"234fg"

        # Test defrag with algorithm
        res = be.defrag(sources, algorithm="sha256")
        expected_hash = hashlib.sha256(b"234fg").hexdigest()
        assert res == expected_hash
        assert be.load(expected_hash) == b"234fg"

        # Test with empty sources
        res = be.defrag([], target="empty")
        assert res == "empty"
        assert be.load("empty") == b""

        # Test error: neither target nor algorithm
        with pytest.raises(ValueError, match="Missing target or algorithm"):
            be.defrag(sources)

        # Test error: unsupported algorithm
        with pytest.raises(ValueError, match="Unsupported hash algorithm"):
            be.defrag(sources, algorithm="invalid")

        # Test defrag with levels=1 and algorithm
        from borgstore.utils.nesting import nest

        res = be.defrag(sources, algorithm="sha256", levels=1)
        expected_hash = hashlib.sha256(b"234fg").hexdigest()
        assert res == nest(expected_hash, levels=1)
        assert be.load(res) == b"234fg"

        # Test defrag with namespace, levels=1 and algorithm
        res = be.defrag(sources, algorithm="sha256", namespace="ns1", levels=1)
        assert res == nest("ns1/" + expected_hash, levels=1)
        assert be.load(res) == b"234fg"

    finally:
        be.close()


def test_rest_content_hash_verification(rest_server_with_auth):
    be = rest_server_with_auth
    base_url = be.base_url + "/"
    auth = be.auth
    headers = {"Accept": "application/vnd.x.borgstore.rest.v1"}

    be.create()
    be.open()
    try:
        # 1. Test store with correct hash
        data1 = b"some data, correct hash"
        correct_hash = hashlib.sha256(data1).hexdigest()
        h = headers.copy()
        h["X-Content-hash-sha256"] = correct_hash

        resp = requests.post(base_url + "item1", data=data1, auth=auth, headers=h)
        assert resp.status_code == 200

        # Verify it was stored
        resp = requests.get(base_url + "item1", auth=auth, headers=headers)
        assert resp.status_code == 200
        assert resp.content == data1

        # 2. Test failed store with incorrect hash
        data2 = b"some data, wrong hash"
        wrong_hash = hashlib.sha256(b"something else").hexdigest()
        h = headers.copy()
        h["X-Content-hash-sha256"] = wrong_hash

        resp = requests.post(base_url + "item2", data=data2, auth=auth, headers=h)
        assert resp.status_code == 422
        assert "Content hash verification failed" in resp.text

        # Verify it was NOT stored
        resp = requests.get(base_url + "item2", auth=auth, headers=headers)
        assert resp.status_code == 404

        # 3. Test store without hash header (should still work)
        data3 = b"some data, no hash"
        resp = requests.post(base_url + "item3", data=data3, auth=auth, headers=headers)
        assert resp.status_code == 200

        resp = requests.get(base_url + "item3", auth=auth, headers=headers)
        assert resp.status_code == 200
        assert resp.content == data3
    finally:
        be.close()


@pytest.fixture
def rest_server_with_quota(tmp_path):
    backend_url = tmp_path.as_uri()
    address, port = "127.0.0.1", 0
    username, password = "testuser", "testpassword"
    quota = 1000  # 1000 bytes

    server, thread = start_server(backend_url, address, port, username, password, quota=quota)

    host, assigned_port = server.server_address
    backend = get_rest_backend(f"http://{username}:{password}@{host}:{assigned_port}/")
    yield backend

    server.shutdown()
    server.server_close()


def test_rest_server_quota_enforced(rest_server_with_quota):
    """Quota is enforced via the REST server: stores within quota succeed, exceeding quota raises QuotaExceeded."""
    be = rest_server_with_quota
    be.create()
    be.open()
    try:
        # Store within quota should succeed
        be.store("obj1", b"x" * 500)
        assert be.load("obj1") == b"x" * 500

        # Store that would exceed quota should fail
        with pytest.raises(QuotaExceeded):
            be.store("obj2", b"x" * 600)

        # Original object should still be intact
        assert be.load("obj1") == b"x" * 500

        # After deleting, we should have room again
        be.delete("obj1")
        be.store("obj3", b"x" * 900)
        assert be.load("obj3") == b"x" * 900
    finally:
        be.close()


def test_rest_server_no_quota(rest_server_with_auth):
    """Without quota, large stores succeed."""
    be = rest_server_with_auth
    be.create()
    be.open()
    try:
        be.store("bigobj", b"x" * 100000)
        assert be.load("bigobj") == b"x" * 100000
    finally:
        be.close()
