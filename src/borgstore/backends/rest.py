"""
REST http client based backend implementation (use with borgstore.server.rest).
"""

import collections
import functools
import os
import re
import shlex
import json
import logging
import hashlib
import threading
import subprocess
import time
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

from ._base import BackendBase, ItemInfo, validate_name, validate_value, StoreValue
from ._utils import make_range_header, ignore_sigint
from .errors import (
    ObjectNotFound,
    BackendAlreadyExists,
    BackendConnectionError,
    BackendDoesNotExist,
    PermissionDenied,
    QuotaExceeded,
    BackendError,
    BackendMustBeOpen,
    BackendMustNotBeOpen,
    ReadRangeError,
)

logger = logging.getLogger(__name__)

# ssh keepalive: make ssh notice a dead peer and terminate (which closes the stdio pipe and unblocks
# our reads) instead of us blocking forever on a read from a connection that will never answer.
# ssh gives up after roughly SSH_ALIVE_INTERVAL * SSH_ALIVE_COUNT_MAX seconds of silence.
SSH_ALIVE_INTERVAL = 30
SSH_ALIVE_COUNT_MAX = 3
# When the connection was lost, try this many times to reconnect and redo the failed operation.
DEFAULT_RECONNECT_TRIES = 3
# Wait this long (seconds) between reconnect attempts.
DEFAULT_RECONNECT_WAIT = 5.0


def _is_connection_lost(exc: BaseException) -> bool:
    """Return True if exc indicates a broken/dead connection (something a reconnect could fix)."""
    if isinstance(exc, BackendConnectionError):
        # raised by StdioSession when the ssh/stdio pipe broke (see request()/close()).
        return True
    if isinstance(exc, (BrokenPipeError, ConnectionError, EOFError)):
        return True
    if requests is not None and isinstance(
        exc,
        (requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.ChunkedEncodingError),
    ):
        # plain http(s) mode: connection reset/timeout while talking to the server.
        return True
    return False


def with_reconnect(method=None, *, swallow_not_found=False):
    """Decorator: if the wrapped method fails because the connection was lost, reconnect and retry.

    This is only active while the backend is opened (has a session). Errors that are not connection
    losses (e.g. ObjectNotFound, PermissionDenied) are passed through unchanged.

    Usable both bare (``@with_reconnect``) and with arguments (``@with_reconnect(...)``).

    swallow_not_found: only for idempotent removals (delete/move). The retry path is only reached
    after a connection loss on an earlier attempt, so if that earlier attempt had already succeeded
    (just the reply got lost), the retried operation raises ObjectNotFound. For delete/move that is
    a spurious error - the desired end state is reached - so we swallow it and report success. On the
    very first attempt ObjectNotFound is a real result and still propagates.
    """
    if method is None:
        return functools.partial(with_reconnect, swallow_not_found=swallow_not_found)

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except Exception as exc:
            if not (self.session is not None and _is_connection_lost(exc)):
                raise
            logger.warning("rest: connection lost (%r), trying to reconnect...", exc)
        last_exc: Optional[BaseException] = None
        for attempt in range(1, self.reconnect_tries + 1):
            time.sleep(self.reconnect_wait)
            try:
                self._reconnect()
            except Exception as exc:
                last_exc = exc
                if not _is_connection_lost(exc):
                    raise
                logger.warning("rest: reconnect attempt %d/%d failed: %r", attempt, self.reconnect_tries, exc)
                continue
            try:
                result = method(self, *args, **kwargs)
            except ObjectNotFound:
                if not swallow_not_found:
                    raise
                # an earlier attempt (before the connection loss) most likely already did it.
                logger.info("rest: reconnected (attempt %d); object already gone, treating as success.", attempt)
                return None
            except Exception as exc:
                last_exc = exc
                if not _is_connection_lost(exc):
                    raise
                logger.warning(
                    "rest: retry after reconnect (attempt %d/%d) failed: %r", attempt, self.reconnect_tries, exc
                )
                continue
            logger.info("rest: reconnected successfully (attempt %d).", attempt)
            return result
        raise BackendError(f"rest connection was lost and could not be reestablished: {last_exc!r}")

    return wrapper


class StdioSession:
    def __init__(self, command, auth=None, headers=None, timeout=30):
        self.command = command
        self.auth = auth
        self.headers = headers or {}
        self.timeout = timeout
        self.process = None
        self._stderr_thread = None
        self._stderr_lines: collections.deque = collections.deque(maxlen=10)  # recent stderr for error messages

    def _drain_stderr(self):
        if self.process is None or self.process.stderr is None:
            return
        for line in self.process.stderr:
            decoded = line.decode("utf-8", errors="replace").rstrip()
            self._stderr_lines.append(decoded)
            logger.debug("Remote: %s", decoded)

    def open(self):
        if self.process is not None:
            return
        self.process = subprocess.Popen(
            self.command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=ignore_sigint,
        )
        self._stderr_thread = threading.Thread(target=self._drain_stderr, daemon=True)
        self._stderr_thread.start()

    def close(self):
        if self.process is None:
            return
        returncode = None
        try:
            if self.process.stdin is not None:
                self.process.stdin.close()
            self.process.wait(timeout=self.timeout)
            returncode = self.process.returncode
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=self.timeout)
        finally:
            if self.process.stdout is not None:
                self.process.stdout.close()
            if self.process.stderr is not None:
                self.process.stderr.close()
            if self._stderr_thread is not None:
                self._stderr_thread.join(timeout=0.5)
            self.process = None
            self._stderr_thread = None
        if returncode:
            stderr_tail = "\n".join(self._stderr_lines)
            detail = f":\n{stderr_tail}" if stderr_tail else ""
            self._stderr_lines.clear()
            raise BackendError(f"stdio server exited with code {returncode}{detail}")
        self._stderr_lines.clear()

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
        try:
            self.process.stdin.write((request_line + header_lines + "\r\n").encode("ascii"))
            if body:
                self.process.stdin.write(body)
            self.process.stdin.flush()
        except (BrokenPipeError, OSError) as e:
            # the stdio pipe broke while sending, i.e. the ssh/server process is gone.
            raise BackendConnectionError(f"stdio connection lost while sending request: {e}") from e

        line = self.process.stdout.readline()
        if not line:
            # EOF: the ssh/server process closed the pipe. With ssh keepalive this is also how a
            # dead network connection surfaces (ssh notices the dead peer and exits). Treat it as a
            # (recoverable) connection loss rather than a plain backend error.
            if self._stderr_thread is not None:
                self._stderr_thread.join(timeout=0.5)
            stderr_tail = "\n".join(self._stderr_lines)
            detail = f":\n{stderr_tail}" if stderr_tail else ""
            raise BackendConnectionError(f"stdio server closed connection unexpectedly{detail}")
        status_line = line.decode("iso-8859-1").strip()
        parts = status_line.split(" ", 2)
        if len(parts) < 2:
            raise BackendError(f"invalid HTTP status line from stdio server: {status_line!r}")
        status_code = int(parts[1])
        reason = parts[2] if len(parts) > 2 else ""

        response_headers = requests.structures.CaseInsensitiveDict()
        while True:
            line = self.process.stdout.readline()
            if not line:
                if self._stderr_thread is not None:
                    self._stderr_thread.join(timeout=0.5)
                stderr_tail = "\n".join(self._stderr_lines)
                detail = f":\n{stderr_tail}" if stderr_tail else ""
                raise BackendConnectionError(
                    f"stdio server closed connection unexpectedly while reading headers{detail}"
                )
            if line in (b"\r\n", b"\n"):
                break
            header_line = line.decode("iso-8859-1").strip()
            if ":" in header_line:
                key, value = header_line.split(":", 1)
                response_headers[key.strip()] = value.strip()

        content_length = int(response_headers.get("Content-Length", "0"))
        response_body = b""
        if method.upper() != "HEAD" and content_length > 0:
            response_body = self.process.stdout.read(content_length)
            if len(response_body) < content_length:
                if self._stderr_thread is not None:
                    self._stderr_thread.join(timeout=0.5)
                stderr_tail = "\n".join(self._stderr_lines)
                detail = f":\n{stderr_tail}" if stderr_tail else ""
                raise BackendConnectionError(
                    f"stdio server closed connection unexpectedly while reading response body: "
                    f"expected {content_length} bytes, got {len(response_body)}{detail}"
                )

        response = requests.Response()
        response.status_code = status_code
        response.headers = response_headers
        response._content = response_body
        response.url = prepared.url
        response.reason = reason
        response.encoding = requests.utils.get_encoding_from_headers(response_headers)
        response.request = prepared
        return response


def ssh_cmd(user, host, port):
    """return an ssh command line that can be prefixed to another command line"""
    rsh = os.environ.get("BORGSTORE_RSH")
    if rsh:
        # a custom remote shell is used verbatim - the user is in control of its options
        # (including any keepalive), so we do not add anything here.
        args = shlex.split(rsh)
    else:
        args = ["ssh"]
        # keepalive: make ssh terminate on a dead peer instead of letting our stdio reads hang forever.
        args += ["-o", f"ServerAliveInterval={SSH_ALIVE_INTERVAL}", "-o", f"ServerAliveCountMax={SSH_ALIVE_COUNT_MAX}"]
        if port:
            args += ["-p", str(port)]
    args += [f"{user}@{host}"] if user else [host]
    return args


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
    # is used to construct a "FILE:" (hack!) backend URI used by the rest server.
    #
    # rest:///path - talk to local rest server, path must be abs. fs path
    # rest://user@host:port/path - ssh to rest server on host, abs. fs path
    rest_regex = r"""
        rest://
        (
            (?:(?P<user>[^@:/]+)@)?  # optional user
            (?P<host>(
                (?!\[)[^:/]+(?<!\])  # hostname or v4 addr, not containing : or / (does not match v6 addr: no brackets!)
                |
                \[[0-9a-fA-F:.]+\])  # ipv6 address in brackets
            )
            (?::(?P<port>\d+))?  # optional port
        )?
        /  # separator always required
        (?P<path>[^?#]+)  # non-empty rel/path or /abs/path or even ~/path or ~user/path
    """
    m = re.match(rest_regex, base_url, re.VERBOSE)
    if m:
        path = m.group("path")
        user = m.group("user")
        host = m.group("host")
        port = m.group("port") or "22"
        # empty host: don't use ssh, just run the rest server here
        command = [] if not host else ssh_cmd(user, host, port)
        # hack: we do NOT use a standards-compliant file:// URI here, because they only support absolute paths.
        # we just use FILE:path and that path can be relative or absolute or even have ~ or ~user.
        # borgstore.server.rest will translate it to an absolute file:// URI internally.
        command.extend(["borgstore-server-rest", "--stdio", "--backend", f"FILE:{path}"])
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
        reconnect_tries: int = DEFAULT_RECONNECT_TRIES,
        reconnect_wait: float = DEFAULT_RECONNECT_WAIT,
    ):
        self.base_url = base_url.rstrip("/")  # _url method adds slash
        self.headers = headers or {}
        self.headers["Accept"] = "application/vnd.x.borgstore.rest.v1"
        self.timeout = timeout
        self.auth = HTTPBasicAuth(username, password) if username and password else None
        self.command = command
        self.reconnect_tries = reconnect_tries
        self.reconnect_wait = reconnect_wait
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
        if response.status_code == HTTP.REQUESTED_RANGE_NOT_SATISFIABLE:
            raise ReadRangeError(response.text)
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

    def _reconnect(self):
        """Drop a (likely broken) session and establish a fresh, working one.

        For stdio mode this restarts the ssh + borgstore-server-rest process; for plain http(s) mode
        it creates a new requests.Session. The server holds no cross-request state, so redoing the
        failed request against the fresh session is safe.
        """
        try:
            if self.session is not None:
                self.session.close()
        except Exception:
            # closing a already-broken session may itself raise (e.g. nonzero ssh exit) - ignore it,
            # we only care about getting a clean, fresh session below.
            pass
        self.session = None
        self.open()

    @with_reconnect
    def mkdir(self, name: str) -> None:
        self._assert_open()
        validate_name(name)
        response = self._request("post", self._url(name), params={"cmd": "mkdir"})
        self._handle_response(response, name)

    @with_reconnect
    def rmdir(self, name: str) -> None:
        self._assert_open()
        validate_name(name)
        response = self._request("delete", self._url(name), params={"cmd": "rmdir"})
        self._handle_response(response, name)

    @with_reconnect
    def info(self, name: str) -> ItemInfo:
        self._assert_open()
        validate_name(name)
        response = self._request("head", self._url(name))
        if response.status_code not in (HTTP.OK, HTTP.NOT_FOUND):
            self._handle_response(response, name)  # raises!
        exists = response.status_code == HTTP.OK
        is_dir = response.headers.get("X-BorgStore-Is-Directory") == "true"
        atime = float(response.headers.get("X-BorgStore-Atime", 0))
        size = int(response.headers.get("Content-Length", 0)) if exists else 0
        return ItemInfo(name=name, exists=exists, size=size, directory=is_dir, atime=atime)

    @with_reconnect
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

    @with_reconnect
    def store(self, name: str, value: StoreValue) -> None:
        self._assert_open()
        validate_name(name)
        # requests sends a memoryview body as it is (with a correct Content-Length), no copy needed.
        value = validate_value(value)
        algorithm = "sha256"
        headers = {f"X-Content-hash-{algorithm}": hashlib.new(algorithm, value).hexdigest()}
        response = self._request("post", self._url(name), data=value, headers=headers)
        self._handle_response(response, name)

    @with_reconnect(swallow_not_found=True)
    def delete(self, name: str) -> None:
        self._assert_open()
        validate_name(name)
        response = self._request("delete", self._url(name))
        self._handle_response(response, name)

    @with_reconnect(swallow_not_found=True)
    def move(self, curr_name: str, new_name: str) -> None:
        self._assert_open()
        validate_name(curr_name)
        validate_name(new_name)
        response = self._request("post", self._url(""), params={"cmd": "move", "current": curr_name, "new": new_name})
        self._handle_response(response, f"{curr_name} -> {new_name}")

    @with_reconnect
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

    @with_reconnect
    def quota(self) -> dict:
        self._assert_open()
        response = self._request("post", self._url(""), params={"cmd": "quota"})
        self._handle_response(response, "quota")
        return response.json()

    @with_reconnect
    def hash(self, name: str, algorithm: str = "sha256") -> str:
        self._assert_open()
        validate_name(name)
        response = self._request("post", self._url(name), params={"cmd": "hash", "algorithm": algorithm})
        self._handle_response(response, name)
        return response.text

    @with_reconnect
    def _list_entries(self, name: str) -> list:
        # separate, decorated helper because .list() is a generator: connection errors would be
        # raised while iterating, i.e. outside the reconnect wrapper. Doing the request (and fully
        # materializing the json) here means the (retryable) network access happens inside with_reconnect.
        self._assert_open()
        validate_name(name)
        response = self._request("get", self._url(name) + "/")  # trailing "/" needed to get list
        self._handle_response(response, name)
        return response.json()

    def list(self, name: str) -> Iterator[ItemInfo]:
        for entry in self._list_entries(name):
            yield ItemInfo(
                name=entry["name"],
                exists=True,
                size=entry["size"],
                directory=entry.get("directory", False),
                atime=entry.get("atime", 0),
            )
