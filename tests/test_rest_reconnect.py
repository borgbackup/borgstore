"""
Tests for the REST backend's connection-loss recovery over stdio-over-ssh (and http).

These tests do not need a real server or ssh: they exercise the with_reconnect decorator, the
_is_connection_lost helper, ssh_cmd keepalive options and StdioSession's EOF handling in isolation.
"""

import io
import os
import signal
import sys
import time

import pytest

requests = pytest.importorskip("requests")

from borgstore.backends.errors import BackendConnectionError, BackendError, ObjectNotFound, PermissionDenied
from borgstore.backends.rest import (
    StdioSession,
    ssh_cmd,
    with_reconnect,
    _is_connection_lost,
    SSH_ALIVE_INTERVAL,
    SSH_ALIVE_COUNT_MAX,
)


@pytest.mark.parametrize(
    "exc, expected",
    [
        (BackendConnectionError("gone"), True),
        (BrokenPipeError(), True),
        (ConnectionResetError(), True),
        (EOFError(), True),
        (requests.exceptions.ConnectionError(), True),
        (requests.exceptions.Timeout(), True),
        (requests.exceptions.ChunkedEncodingError(), True),
        # legit results / unrelated errors, must NOT be treated as connection loss:
        (ObjectNotFound("x"), False),
        (PermissionDenied("x"), False),
        (BackendError("some other backend error"), False),
        (ValueError("nope"), False),
    ],
)
def test_is_connection_lost(exc, expected):
    assert _is_connection_lost(exc) is expected


class FakeREST:
    """Minimal stand-in providing exactly what with_reconnect / _reconnect touch."""

    def __init__(self, reconnect_tries=3):
        self.session = object()  # "opened" (non-None)
        self.reconnect_tries = reconnect_tries
        self.reconnect_wait = 0  # do not slow down the tests
        self.reconnects = 0

    def _reconnect(self):
        self.reconnects += 1
        self.session = object()  # fresh session


def test_retry_succeeds_after_reconnect():
    obj = FakeREST()
    calls = {"n": 0}

    @with_reconnect
    def op(self):
        calls["n"] += 1
        if calls["n"] == 1:
            raise BackendConnectionError("dropped")
        return "ok"

    assert op(obj) == "ok"
    assert obj.reconnects == 1
    assert calls["n"] == 2


def test_no_retry_for_non_connection_error():
    obj = FakeREST()

    @with_reconnect
    def op(self):
        raise ObjectNotFound("missing")

    with pytest.raises(ObjectNotFound):
        op(obj)
    assert obj.reconnects == 0


def test_gives_up_after_all_tries():
    obj = FakeREST(reconnect_tries=3)

    @with_reconnect
    def op(self):
        raise BackendConnectionError("still down")

    with pytest.raises(BackendError):
        op(obj)
    assert obj.reconnects == 3


def test_not_opened_does_not_reconnect():
    obj = FakeREST()
    obj.session = None  # not opened

    @with_reconnect
    def op(self):
        raise BackendConnectionError("dropped")

    with pytest.raises(BackendConnectionError):
        op(obj)
    assert obj.reconnects == 0


def test_swallow_not_found_on_retry():
    """delete/move: ObjectNotFound after a reconnect means the earlier attempt already did it."""
    obj = FakeREST()
    calls = {"n": 0}

    @with_reconnect(swallow_not_found=True)
    def op(self):
        calls["n"] += 1
        if calls["n"] == 1:
            raise BackendConnectionError("dropped")  # op may already have succeeded
        raise ObjectNotFound("gone")  # retry: already gone -> treat as success

    assert op(obj) is None
    assert obj.reconnects == 1


def test_object_not_found_propagates_on_first_attempt_even_when_swallowing():
    obj = FakeREST()

    @with_reconnect(swallow_not_found=True)
    def op(self):
        raise ObjectNotFound("gone")

    with pytest.raises(ObjectNotFound):
        op(obj)
    assert obj.reconnects == 0


def test_ssh_cmd_has_keepalive_by_default(monkeypatch):
    monkeypatch.delenv("BORGSTORE_RSH", raising=False)
    cmd = ssh_cmd("user", "host", "2222")
    assert cmd[0] == "ssh"
    assert f"ServerAliveInterval={SSH_ALIVE_INTERVAL}" in cmd
    assert f"ServerAliveCountMax={SSH_ALIVE_COUNT_MAX}" in cmd
    assert "-p" in cmd and "2222" in cmd
    assert cmd[-1] == "user@host"


def test_ssh_cmd_custom_rsh_is_verbatim(monkeypatch):
    monkeypatch.setenv("BORGSTORE_RSH", "ssh -F /my/config")
    cmd = ssh_cmd("user", "host", "22")
    # a custom rsh is used as-is: we do not inject keepalive options.
    assert cmd[:3] == ["ssh", "-F", "/my/config"]
    assert not any("ServerAlive" in part for part in cmd)
    assert cmd[-1] == "user@host"


class _FakeStdin:
    def __init__(self):
        self.buffer = bytearray()

    def write(self, data):
        self.buffer.extend(data)

    def flush(self):
        pass


class _FakeStdout:
    def __init__(self, data=b""):
        self._io = io.BytesIO(data)

    def readline(self):
        return self._io.readline()

    def read(self, n):
        return self._io.read(n)


class _FakeProc:
    def __init__(self, stdout_data=b""):
        self.stdin = _FakeStdin()
        self.stdout = _FakeStdout(stdout_data)


def test_stdio_request_eof_raises_connection_error():
    """When the server/ssh closed the pipe (readline -> b''), request() raises a connection error."""
    session = StdioSession(command=["true"])
    session.process = _FakeProc(stdout_data=b"")  # immediate EOF on the response

    with pytest.raises(BackendConnectionError):
        session.request("GET", "http://stdio-backend/item")


def test_stdio_request_broken_pipe_raises_connection_error():
    """A broken pipe while sending the request is reported as a connection error."""
    session = StdioSession(command=["true"])
    proc = _FakeProc()

    def boom(_data):
        raise BrokenPipeError()

    proc.stdin.write = boom
    session.process = proc

    with pytest.raises(BackendConnectionError):
        session.request("GET", "http://stdio-backend/item")


def test_stdio_request_eof_during_headers_raises_connection_error():
    """Unexpected EOF while reading headers is reported as a connection error."""
    session = StdioSession(command=["true"])
    session.process = _FakeProc(stdout_data=b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n")

    with pytest.raises(BackendConnectionError) as exc_info:
        session.request("GET", "http://stdio-backend/item")
    assert "reading headers" in str(exc_info.value)


def test_stdio_request_eof_during_body_raises_connection_error():
    """Unexpected EOF while reading body (shorter than Content-Length) is reported as a connection error."""
    session = StdioSession(command=["true"])
    session.process = _FakeProc(stdout_data=b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n123")

    with pytest.raises(BackendConnectionError) as exc_info:
        session.request("GET", "http://stdio-backend/item")
    assert "reading response body" in str(exc_info.value)


# a minimal stdio "server": answers each request with "ok", after writing more to stderr than a pipe buffer holds.
STDERR_FLOOD_SERVER = r"""
import sys
while sys.stdin.buffer.readline():
    while sys.stdin.buffer.readline() not in (b"\r\n", b""):
        pass
    sys.stderr.buffer.write((b"y" * 99 + b"\n") * 2000)
    sys.stderr.buffer.flush()
    sys.stdout.buffer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
    sys.stdout.buffer.flush()
"""


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")  # fork() of a multi-threaded process
def test_stdio_session_works_in_forked_child():
    """A session opened before daemonizing (e.g. borg mount) works in the daemon: stderr is still drained
    (so a server writing lots to stderr does not block) and close() does not hang."""
    r, w = os.pipe()
    pid = os.fork()
    if pid == 0:  # like borg mount before daemonizing
        try:
            os.close(r)
            session = StdioSession(command=[sys.executable, "-c", STDERR_FLOOD_SERVER])
            session.open()
            session.request("GET", "http://stdio-backend/item")
            time.sleep(0.5)  # let the stderr thread block in read(), holding the buffered reader's lock
            parent_pid = os.getpid()
            if os.fork() == 0:  # the daemon: the stderr thread does not exist here
                signal.alarm(10)  # kill the daemon if it hangs (closes w, so the test reads EOF)
                result = b"fail"
                try:
                    while os.getppid() == parent_pid:
                        time.sleep(0.01)  # wait until our parent (and its stderr thread) is gone
                    if session.request("GET", "http://stdio-backend/item").content == b"ok":
                        session.close()
                        result = b"ok"
                finally:
                    os.write(w, result)
                    os._exit(0)
        finally:
            os._exit(0)
    os.close(w)
    os.waitpid(pid, 0)
    with os.fdopen(r, "rb") as f:
        assert f.read() == b"ok"
