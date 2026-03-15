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
from borgstore.backends.errors import ObjectNotFound, BackendAlreadyExists


def start_server(backend_url, address, port, username=None, password=None, permissions=None):
    from borgstore.store import get_backend

    backend = get_backend(backend_url, permissions=permissions)
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
