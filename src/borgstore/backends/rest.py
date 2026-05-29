"""
REST http client based backend implementation (use with borgstore.server.rest).
"""

import os
import re
import json
import sys
import logging
import hashlib
import threading
import subprocess
from typing import Iterator, Dict, Optional
from types import ModuleType
from http import HTTPStatus as HTTP
from urllib.parse import unquote

requests: Optional[ModuleType] = None
HTTPBasicAuth: Optional[type] = None
try:
    import requests as requests_module
    from requests.auth import HTTPBasicAuth as HTTPBasicAuth_class

    requests = requests_module
    HTTPBasicAuth = HTTPBasicAuth_class
except ImportError:
    pass

from ._base import BackendBase, ItemInfo, validate_name
from ._utils import make_range_header
from .errors import (
    ObjectNotFound,
    BackendAlreadyExists,
    BackendDoesNotExist,
    PermissionDenied,
    QuotaExceeded,
    BackendError,
    BackendMustBeOpen,
    BackendMustNotBeOpen,
)

logger = logging.getLogger(__name__)


class StdioSession:
    def __init__(self, command, auth=None, headers=None, timeout=30):
        self.command = command
        self.auth = auth
        self.headers = headers or {}
        self.timeout = timeout
        self.process = None
        self._stderr_thread = None

    def _drain_stderr(self):
        if self.process is None or self.process.stderr is None:
            return
        for line in self.process.stderr:
            logger.warning("REST stdio server: %s", line.decode("utf-8", errors="replace").rstrip())

    def open(self):
        if self.process is not None:
            return
        self.process = subprocess.Popen(
            self.command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        self._stderr_thread = threading.Thread(target=self._drain_stderr, daemon=True)
        self._stderr_thread.start()

    def close(self):
        if self.process is None:
            return
        try:
            if self.process.stdin is not None:
                self.process.stdin.close()
            self.process.wait(timeout=self.timeout)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=self.timeout)
        finally:
            if self.process.stdout is not None:
                self.process.stdout.close()
            if self.process.stderr is not None:
                self.process.stderr.close()
            self.process = None
            self._stderr_thread = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def request(self, method, url, params=None, data=None, headers=None, timeout=None):
        if self.process is None or self.process.stdin is None or self.process.stdout is None:
            raise BackendError("stdio session is not open")

        request_headers = dict(self.headers)
        if headers:
            request_headers.update(headers)
        request_headers["Connection"] = "keep-alive"

        prepared = requests.Request(
            method=method, url=url, params=params, data=data, headers=request_headers, auth=self.auth
        ).prepare()

        body = prepared.body
        if body is None:
            body = b""
        elif isinstance(body, bytes):
            pass  # ok
        elif isinstance(body, str):
            body = body.encode("utf-8")
        else:
            raise BackendError(f"unsupported body type: {type(body).__name__}")

        request_line = f"{prepared.method} {prepared.path_url} HTTP/1.1\r\n"
        header_lines = "".join(f"{k}: {v}\r\n" for k, v in prepared.headers.items())
        self.process.stdin.write((request_line + header_lines + "\r\n").encode("ascii"))
        if body:
            self.process.stdin.write(body)
        self.process.stdin.flush()

        line = self.process.stdout.readline()
        if not line:
            raise BackendError("stdio server closed connection unexpectedly")
        status_line = line.decode("iso-8859-1").strip()
        parts = status_line.split(" ", 2)
        if len(parts) < 2:
            raise BackendError(f"invalid HTTP status line from stdio server: {status_line!r}")
        status_code = int(parts[1])
        reason = parts[2] if len(parts) > 2 else ""

        response_headers = requests.structures.CaseInsensitiveDict()
        while True:
            line = self.process.stdout.readline()
            if line in (b"\r\n", b"\n", b""):
                break
            header_line = line.decode("iso-8859-1").strip()
            if ":" in header_line:
                key, value = header_line.split(":", 1)
                response_headers[key.strip()] = value.strip()

        content_length = int(response_headers.get("Content-Length", "0"))
        response_body = self.process.stdout.read(content_length) if content_length else b""

        response = requests.Response()
        response.status_code = status_code
        response.headers = response_headers
        response._content = response_body
        response.url = prepared.url
        response.reason = reason
        response.encoding = requests.utils.get_encoding_from_headers(response_headers)
        response.request = prepared
        return response


def get_rest_backend(base_url: str):
    if not base_url.startswith(("http:", "https:", "rest:")):
        return None

    if requests is None:
        raise BackendDoesNotExist(
            "The REST backend requires dependencies. Install them with: 'pip install borgstore[rest]'"
        )

    # http(s)://username:password@hostname:port/sub/path or
    # http(s)://hostname:port/sub/path + authentication from environment
    #
    # note: borgstore.server.rest does not support sub-paths, but sub-paths are
    # supported in the rest client for use with reverse-proxy setups (see contrib/)
    # or custom REST servers.
    http_regex = r"""
        (?P<scheme>http|https)://
        ((?P<username>[^:]+):(?P<password>[^@]+)@)?
        (?P<host>[^:/]+)(:(?P<port>\d+))?
        (?P<path>/[^?#]*)?
    """
    m = re.match(http_regex, base_url, re.VERBOSE)
    if m:
        scheme = m.group("scheme")
        host = m.group("host")
        port = m.group("port")
        path = m.group("path") or ""

        base_url = f"{scheme}://{host}{f':{port}' if port else ''}{path}"

        username, password = m.group("username"), m.group("password")
        if username and password:
            username, password = unquote(username), unquote(password)
        else:
            username, password = os.environ.get("BORGSTORE_REST_USERNAME"), os.environ.get("BORGSTORE_REST_PASSWORD")

        return REST(base_url, username=username, password=password)

    # rest protocol means: use stdio to talk to a borgstore.server.rest process,
    # either locally (empty host) or via ssh to the given host. The given path
    # is used to construct a "file:" backend URL used by the rest server.
    #
    # rest:///path - talk to local rest server, path must be abs. fs path
    # rest://user@host:port/path - ssh to rest server on host, abs. fs path
    rest_regex = r"""
        rest://
        (((?P<user>[^@]+)@)(?P<host>[^:/]+)(:(?P<port>\d+))?)?
        /  # separator always required
        (?P<path>/[^?#]*)  # absolute path for now
    """
    m = re.match(rest_regex, base_url, re.VERBOSE)
    if m:
        path = m.group("path")
        user = m.group("user")
        host = m.group("host")
        port = m.group("port") or "22"
        if not host:
            # empty host: don't use ssh, just run the rest server here
            command = []
            python = sys.executable
        else:
            command = ["ssh", "-p", port, f"{user}@{host}"]
            python = "python3"
        command.extend([python, "-m", "borgstore.server.rest", "--stdio", "--backend", f"file://{path}"])
        return REST(base_url="http://stdio-backend", command=command)


class REST(BackendBase):
    def __init__(
        self,
        base_url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = 30,
        command=None,
    ):
        self.base_url = base_url.rstrip("/")  # _url method adds slash
        self.headers = headers or {}
        self.headers["Accept"] = "application/vnd.x.borgstore.rest.v1"
        self.timeout = timeout
        self.auth = HTTPBasicAuth(username, password) if username and password else None
        self.command = command
        self.session = None

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    def _assert_open(self):
        if self.session is None:
            raise BackendMustBeOpen()

    def _assert_closed(self):
        if self.session is not None:
            raise BackendMustNotBeOpen()

    def _request(self, method, url, *, headers=None, data=None, params=None):
        if self.session is not None:  # between .open() and .close()
            return self.session.request(method, url, params=params, data=data, headers=headers, timeout=self.timeout)
        else:  # .create() and .destroy() are called when backend is not opened
            if headers is not None:
                raise ValueError("custom headers are not supported outside of an open session")
            if self.command is not None:
                with StdioSession(
                    command=self.command, auth=self.auth, headers=self.headers, timeout=self.timeout
                ) as session:
                    return session.request(method, url, params=params, data=data, timeout=self.timeout)
            return requests.request(
                method, url, auth=self.auth, params=params, data=data, headers=self.headers, timeout=self.timeout
            )

    def _handle_response(self, response, name=None):
        if response.status_code == HTTP.OK:
            return
        if response.status_code == HTTP.PARTIAL_CONTENT:
            return
        if response.status_code == HTTP.NOT_FOUND:
            raise ObjectNotFound(name or "unknown")
        if response.status_code == HTTP.GONE:
            raise BackendDoesNotExist(self.base_url)
        if response.status_code == HTTP.CONFLICT:
            raise BackendAlreadyExists(self.base_url)
        if response.status_code == HTTP.PRECONDITION_FAILED:
            # Precondition failed, used for state errors
            if "must be open" in response.text:
                raise BackendMustBeOpen()
            if "must not be open" in response.text:
                raise BackendMustNotBeOpen()
            raise BackendError(response.text)
        if response.status_code == HTTP.FORBIDDEN:
            raise PermissionDenied(name or self.base_url)
        if response.status_code == HTTP.INSUFFICIENT_STORAGE:
            raise QuotaExceeded(response.text)
        if response.status_code == HTTP.BAD_REQUEST:
            raise ValueError(response.text)
        response.raise_for_status()

    def create(self) -> None:
        self._assert_closed()
        response = self._request("post", self._url(""), params={"cmd": "create"})
        self._handle_response(response, "backend")

    def destroy(self) -> None:
        self._assert_closed()
        response = self._request("delete", self._url(""), params={"cmd": "destroy"})
        self._handle_response(response, "backend")

    def open(self):
        self._assert_closed()
        if self.command is not None:
            self.session = StdioSession(
                command=self.command, auth=self.auth, headers=self.headers, timeout=self.timeout
            )
            self.session.open()
        else:
            self.session = requests.Session()
            self.session.auth = self.auth
            self.session.headers.update(self.headers)

    def close(self):
        self._assert_open()
        self.session.close()
        self.session = None

    def mkdir(self, name: str) -> None:
        self._assert_open()
        validate_name(name)
        response = self._request("post", self._url(name), params={"cmd": "mkdir"})
        self._handle_response(response, name)

    def rmdir(self, name: str) -> None:
        self._assert_open()
        validate_name(name)
        response = self._request("delete", self._url(name), params={"cmd": "rmdir"})
        self._handle_response(response, name)

    def info(self, name: str) -> ItemInfo:
        self._assert_open()
        validate_name(name)
        response = self._request("head", self._url(name))
        if response.status_code not in (HTTP.OK, HTTP.NOT_FOUND):
            self._handle_response(response, name)  # raises!
        exists = response.status_code == HTTP.OK
        is_dir = response.headers.get("X-BorgStore-Is-Directory") == "true"
        return ItemInfo(name=name, exists=exists, size=int(response.headers.get("Content-Length", 0)), directory=is_dir)

    def load(self, name: str, *, size=None, offset=0) -> bytes:
        self._assert_open()
        validate_name(name)

        if offset < 0 and size is not None:
            if -offset - size <= 1024:
                # Optimization: if the part of the tail we don't need is small,
                # we just request the last N bytes and truncate locally.
                range_header = make_range_header(offset, size=None)
            else:
                info = self.info(name)
                range_header = make_range_header(offset, size, info.size)
        else:
            range_header = make_range_header(offset, size)

        headers = self.headers.copy()
        if range_header:
            headers["Range"] = range_header

        response = self._request("get", self._url(name), headers=headers)
        self._handle_response(response, name)
        content = response.content
        if offset < 0 and size is not None and size < len(content):
            content = content[:size]
        return content

    def store(self, name: str, value: bytes) -> None:
        self._assert_open()
        validate_name(name)
        algorithm = "sha256"
        headers = {f"X-Content-hash-{algorithm}": hashlib.new(algorithm, value).hexdigest()}
        response = self._request("post", self._url(name), data=value, headers=headers)
        self._handle_response(response, name)

    def delete(self, name: str) -> None:
        self._assert_open()
        validate_name(name)
        response = self._request("delete", self._url(name))
        self._handle_response(response, name)

    def move(self, curr_name: str, new_name: str) -> None:
        self._assert_open()
        validate_name(curr_name)
        validate_name(new_name)
        response = self._request("post", self._url(""), params={"cmd": "move", "current": curr_name, "new": new_name})
        self._handle_response(response, f"{curr_name} -> {new_name}")

    def defrag(self, sources, *, target=None, algorithm=None, namespace=None, levels=0) -> str:
        self._assert_open()
        params = {"cmd": "defrag"}
        if target is not None:
            params["target"] = target
        if algorithm is not None:
            params["algorithm"] = algorithm
        if namespace is not None:
            params["namespace"] = namespace
        if levels:
            params["levels"] = levels
        data = json.dumps(sources).encode("utf-8")
        response = self._request("post", self._url(""), params=params, data=data)
        self._handle_response(response, "defrag")
        return response.text

    def quota(self) -> dict:
        self._assert_open()
        response = self._request("post", self._url(""), params={"cmd": "quota"})
        self._handle_response(response, "quota")
        return response.json()

    def hash(self, name: str, algorithm: str = "sha256") -> str:
        self._assert_open()
        validate_name(name)
        response = self._request("post", self._url(name), params={"cmd": "hash", "algorithm": algorithm})
        self._handle_response(response, name)
        return response.text

    def list(self, name: str) -> Iterator[ItemInfo]:
        self._assert_open()
        validate_name(name)
        response = self._request("get", self._url(name) + "/")  # trailing "/" needed to get list
        self._handle_response(response, name)
        for entry in response.json():
            yield ItemInfo(name=entry["name"], exists=True, size=entry["size"], directory=entry.get("directory", False))
