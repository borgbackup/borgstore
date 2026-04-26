import secrets
import hashlib
import argparse
import json
import base64
import logging
import os
import socket
import itertools
from http import HTTPStatus as HTTP
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlsplit, parse_qs

from ..backends.errors import (
    ObjectNotFound,
    BackendAlreadyExists,
    BackendDoesNotExist,
    PermissionDenied,
    QuotaExceeded,
    BackendError,
    BackendMustBeOpen,
    BackendMustNotBeOpen,
)
from ..backends._utils import parse_range_header
from ..store import get_backend

logger = logging.getLogger(__name__)


class BorgStoreRESTRequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    # https://en.wikipedia.org/wiki/List_of_Unicode_characters#Control_codes
    _control_char_table = str.maketrans({c: rf"\x{c:02x}" for c in itertools.chain(range(0x20), range(0x7F, 0xA0))})
    _control_char_table[ord("\\")] = r"\\"

    def address_string(self):
        # Override to handle Unix domain sockets (AF_UNIX).
        # BaseHTTPRequestHandler.address_string() assumes client_address is a tuple (host, port).
        # For AF_UNIX, client_address is a string (the path), which can be empty.
        if isinstance(self.client_address, str):
            return self.client_address or "unix"
        return super().address_string()

    def _log(self, format, args, level=logging.INFO):
        addr = self.address_string()
        dt = self.log_date_time_string()
        user = self.server.username or "-"
        request_details = format % args
        msg = f"{addr} - {user} [{dt}] {request_details}"
        logger.log(level, msg.translate(self._control_char_table))

    def log_message(self, format, *args):
        self._log(format, args, logging.INFO)

    def log_error(self, format, *args):
        # usually this is pretty useless and redundant, thus we only log it at debug level.
        self._log(format, args, logging.DEBUG)

    @staticmethod
    def checks_and_logging(func):
        def wrapper(self):
            if not self._check_accept():
                return
            if not self._check_auth():
                return self._send_unauthorized()
            return func(self)

        return wrapper

    def _check_auth(self):
        if not self.server.username or not self.server.password:
            return True
        auth_header = self.headers.get("Authorization")
        if not auth_header:
            return False
        scheme, _, encoded_credentials = auth_header.partition(" ")
        if scheme.lower() != "basic":
            return False
        try:
            decoded_credentials = base64.b64decode(encoded_credentials).decode("utf-8")
            username, _, password = decoded_credentials.partition(":")
            authorized = secrets.compare_digest(username, self.server.username) and secrets.compare_digest(
                password, self.server.password
            )
            return authorized
        except Exception:
            logger.exception("Authentication code crashed, returning: unauthorized.")
            return False

    def respond(self, status=HTTP.OK, data=None, content_type=None, headers=None):
        self.send_response(status)
        if content_type:
            self.send_header("Content-Type", content_type)
        # Ensure no proxy or client caches our REST responses.
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        if headers:
            for key, value in headers.items():
                self.send_header(key, value)
        if data is not None:
            self.send_header("Content-Length", str(len(data)))
        elif not headers or "Content-Length" not in headers:
            self.send_header("Content-Length", "0")
        self.end_headers()
        if data is not None and self.command != "HEAD":
            self.wfile.write(data)

    def _send_unauthorized(self):
        self.respond(
            HTTP.UNAUTHORIZED, data=b"Unauthorized", headers={"WWW-Authenticate": 'Basic realm="BorgStore REST Server"'}
        )

    def _check_accept(self):
        accept = self.headers.get("Accept")
        if accept != "application/vnd.x.borgstore.rest.v1":
            msg = "Not Acceptable: unsupported or missing Accept header"
            self.send_error(HTTP.NOT_ACCEPTABLE, msg)
            return False
        return True

    @property
    def split_url(self):
        return urlsplit(self.path)

    @property
    def query(self):
        return parse_qs(self.split_url.query)

    @property
    def name(self):
        return self.split_url.path.strip("/")

    def _handle_exception(self, e, name=None):
        msg = str(e)
        # Security: do not leak absolute paths in error messages
        for attr in ("base_path", "fs"):
            if self.server.backend and hasattr(self.server.backend, attr):
                path_val = str(getattr(self.server.backend, attr))
                if path_val and path_val in msg:
                    msg = msg.replace(path_val, "[STORAGE_BASE]")

        if isinstance(e, ObjectNotFound):
            self.send_error(HTTP.NOT_FOUND, msg)
        elif isinstance(e, BackendDoesNotExist):
            self.send_error(HTTP.GONE, msg)
        elif isinstance(e, BackendAlreadyExists):
            self.send_error(HTTP.CONFLICT, msg)
        elif isinstance(e, (BackendMustBeOpen, BackendMustNotBeOpen)):
            self.send_error(HTTP.PRECONDITION_FAILED, msg)
        elif isinstance(e, PermissionDenied):
            self.send_error(HTTP.FORBIDDEN, msg)
        elif isinstance(e, QuotaExceeded):
            self.send_error(HTTP.INSUFFICIENT_STORAGE, msg)
        elif isinstance(e, (ValueError, TypeError)):
            self.send_error(HTTP.BAD_REQUEST, msg)
            logger.exception("Exception for %s", name or self.path)
        elif isinstance(e, BackendError):
            self.send_error(HTTP.INTERNAL_SERVER_ERROR, msg)
            logger.exception("Exception for %s", name or self.path)
        else:
            self.send_error(HTTP.INTERNAL_SERVER_ERROR, "Internal Server Error")
            logger.exception("Exception for %s", name or self.path)

    @checks_and_logging
    def do_POST(self):
        cmd = self.query.get("cmd", [None])[0]
        if cmd == "create":
            try:
                self.server.backend.create()
                self.respond(HTTP.OK)
            except Exception as e:
                self._handle_exception(e, "create")
            return

        if cmd == "move":
            current = self.query.get("current", [None])[0]
            new = self.query.get("new", [None])[0]
            if current and new:
                try:
                    with self.server.backend:
                        self.server.backend.move(current, new)
                    self.respond(HTTP.OK)
                except Exception as e:
                    self._handle_exception(e, f"move {current} -> {new}")
            else:
                self.send_error(HTTP.BAD_REQUEST, "Missing current or new name for move")
            return

        if cmd == "mkdir":
            try:
                with self.server.backend:
                    self.server.backend.mkdir(self.name)
                self.respond(HTTP.OK)
            except Exception as e:
                self._handle_exception(e, f"mkdir {self.name}")
            return

        if cmd == "hash":
            if not self.name:
                self.send_error(HTTP.BAD_REQUEST, "Missing name for hash")
                return
            algorithm = self.query.get("algorithm", ["sha256"])[0]
            try:
                with self.server.backend:
                    digest = self.server.backend.hash(self.name, algorithm=algorithm)
                self.respond(HTTP.OK, data=digest.encode("ascii"), content_type="text/plain")
            except Exception as e:
                self._handle_exception(e, f"hash {self.name}")
            return

        if cmd == "quota":
            try:
                with self.server.backend:
                    quota_info = self.server.backend.quota()
                response_data = json.dumps(quota_info).encode("utf-8")
                self.respond(HTTP.OK, data=response_data, content_type="application/json")
            except Exception as e:
                self._handle_exception(e, "quota")
            return

        if cmd == "defrag":
            target = self.query.get("target", [None])[0]
            algorithm = self.query.get("algorithm", [None])[0]
            namespace = self.query.get("namespace", [None])[0]
            levels = int(self.query.get("levels", [0])[0])
            if not target and not algorithm:
                self.send_error(HTTP.BAD_REQUEST, "Missing target or algorithm for defrag")
                return
            try:
                content_length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_length)
                sources = json.loads(body)
                with self.server.backend:
                    target = self.server.backend.defrag(
                        sources, target=target, algorithm=algorithm, namespace=namespace, levels=levels
                    )
                self.respond(HTTP.OK, data=target.encode("ascii"), content_type="text/plain")
            except ValueError as e:
                self.send_error(HTTP.BAD_REQUEST, str(e))
            except Exception as e:
                self._handle_exception(e, "defrag")
            return

        if self.name:
            try:
                content_length = int(self.headers.get("Content-Length", 0))
                algorithm = "sha256"
                expected_hash = self.headers.get(f"X-Content-hash-{algorithm}")
                data = self.rfile.read(content_length)
                if expected_hash:
                    got_hash = hashlib.new(algorithm, data).hexdigest()
                    if got_hash != expected_hash:
                        self.respond(HTTP.UNPROCESSABLE_ENTITY, b"Content hash verification failed, please retry")
                        return
                with self.server.backend:
                    self.server.backend.store(self.name, data)
                self.respond(HTTP.OK)
            except Exception as e:
                self._handle_exception(e, self.name)
            return

        self.send_error(HTTP.BAD_REQUEST, "Bad Request")

    @checks_and_logging
    def do_DELETE(self):
        cmd = self.query.get("cmd", [None])[0]
        if cmd == "rmdir":
            try:
                with self.server.backend:
                    self.server.backend.rmdir(self.name)
                self.respond(HTTP.OK)
            except Exception as e:
                self._handle_exception(e, f"rmdir {self.name}")
            return

        if cmd == "destroy":
            try:
                self.server.backend.destroy()
                self.respond(HTTP.OK)
            except Exception as e:
                self._handle_exception(e, "destroy")
            return

        if not self.name:
            self.send_error(HTTP.BAD_REQUEST, "Bad Request")
            return

        try:
            with self.server.backend:
                self.server.backend.delete(self.name)
            self.respond(HTTP.OK)
        except Exception as e:
            self._handle_exception(e, self.name)

    @checks_and_logging
    def do_HEAD(self):
        if not self.name:
            self.send_error(HTTP.BAD_REQUEST, "Bad Request")
            return

        try:
            with self.server.backend:
                info = self.server.backend.info(self.name)
            if not info.exists:
                raise ObjectNotFound(self.name)
            self.respond(
                HTTP.OK,
                headers={
                    "Content-Length": str(info.size),
                    "X-BorgStore-Is-Directory": "true" if info.directory else "false",
                },
            )
        except Exception as e:
            self._handle_exception(e, self.name)

    @checks_and_logging
    def do_GET(self):
        # List directory
        if self.split_url.path.endswith("/"):
            try:
                # send a JSON list of objects
                # [{"name": "...", "size": ...}, ...]
                with self.server.backend:
                    items = (
                        {"name": item.name, "size": item.size, "directory": item.directory}
                        for item in self.server.backend.list(self.name)
                    )
                    json_data = json.dumps(list(items), indent=2)
                response_data = json_data.encode("utf-8")
                self.respond(HTTP.OK, data=response_data, content_type="application/json")
            except Exception as e:
                self._handle_exception(e, self.name)
            return

        # Load object
        if not self.name:
            self.send_error(HTTP.BAD_REQUEST, "Bad Request")
            return

        try:
            range_header = self.headers.get("Range")
            offset, size = parse_range_header(range_header) if range_header else (0, None)

            with self.server.backend:
                data = self.server.backend.load(self.name, offset=offset, size=size)
            self.respond(
                HTTP.PARTIAL_CONTENT if range_header else HTTP.OK, data=data, content_type="application/octet-stream"
            )
        except Exception as e:
            self._handle_exception(e, self.name)


def get_pre_bound_socket():
    """Return pre-bound socket passed by systemd via socket activation.

    Reads LISTEN_FDS from the environment (set by systemd) and wraps each
    raw file descriptor (starting at fd 3) as a socket.socket object.

    See sd_listen_fds(3) for the protocol.
    """
    n = int(os.environ.get("LISTEN_FDS", 0))
    if n == 0:
        raise RuntimeError(
            "--socket-activation was requested but no sockets were passed by systemd (LISTEN_FDS not set or 0)"
        )
    if n > 1:
        raise RuntimeError(f"--socket-activation expects exactly 1 socket from systemd, got {n}")
    # SD_LISTEN_FDS_START is always 3. The socket is a Unix domain socket
    # (as configured in borgstore@.socket), so use AF_UNIX / SOCK_STREAM.
    return socket.fromfd(3, socket.AF_UNIX, socket.SOCK_STREAM)


class BorgStoreRESTServer(ThreadingHTTPServer):
    """
    BorgStore REST Server.

    Security Warning:
    This server does not implement TLS. In a production environment, it SHOULD
    be run behind a reverse proxy (like Nginx or Caddy) that provides HTTPS.
    """

    disable_nagle_algorithm = True  # aka TCP_NODELAY, reduces latency

    def __init__(self, server_address, backend, username=None, password=None, adopted_socket=None):
        self.backend = backend
        self.username = username
        self.password = password
        if adopted_socket is not None:
            # Socket activation: systemd already bound and is listening on adopted_socket.
            #
            # TCPServer.__init__ unconditionally creates self.socket = socket.socket(...)
            # regardless of bind_and_activate, so we cannot set self.socket before calling
            # super().__init__. The correct sequence is:
            #   1. Call super().__init__ with bind_and_activate=False so it sets up
            #      internal state (but also creates a fresh, unbound socket).
            #   2. Close and discard that fresh socket.
            #   3. Replace self.socket with the systemd-provided one.
            #   4. Read back server_address from the socket (getsockname()).
            #   Do NOT call server_bind() or server_activate() — the socket is already
            #   bound and listening; calling bind() again raises EADDRINUSE.
            self.address_family = socket.AF_UNIX
            # Unix sockets do not support TCP_NODELAY.
            self.disable_nagle_algorithm = False
            super().__init__(server_address, BorgStoreRESTRequestHandler, bind_and_activate=False)
            self.socket.close()  # discard the socket super() created
            self.socket = adopted_socket  # install the systemd socket
            self.server_address = self.socket.getsockname()
            # HTTPServer.server_bind usually sets these. We set them manually for AF_UNIX.
            self.server_name = "unix-socket"
            self.server_port = 0
        else:
            super().__init__(server_address, BorgStoreRESTRequestHandler)

    def handle_error(self, request, client_address):
        # Ensure all errors are logged to the journal so we can see them in CI.
        logger.exception(f"Exception occurred during processing of request from {client_address}")
        super().handle_error(request, client_address)


PERMISSION_SHORTCUTS = {
    # these are for borgbackup, see borg.repository.Repository.__init__
    "borgbackup-all": None,  # permissions system will not be used
    "borgbackup-no-delete": {  # mostly no delete, no overwrite
        "": "lr",
        "archives": "lrw",
        "cache": "lrwWD",  # WD for chunks.<HASH>, last-key-checked, ...
        "config": "lrW",  # W for manifest
        "data": "lrw",
        "keys": "lr",
        "locks": "lrwD",  # borg needs to create/delete a shared lock here
    },
    "borgbackup-write-only": {  # mostly no reading
        "": "l",
        "archives": "lw",
        "cache": "lrwWD",  # read allowed, e.g. for chunks.<HASH> cache
        "config": "lrW",  # W for manifest
        "data": "lw",  # no r!
        "keys": "lr",
        "locks": "lrwD",  # borg needs to create/delete a shared lock here
    },
    "borgbackup-read-only": {"": "lr", "locks": "lrwD"},  # mostly r/o
}


def resolve_permissions(permissions):
    """Resolve a permissions shortcut name or JSON string to a permissions dict (or None)."""
    if permissions is None:
        return None
    if permissions in PERMISSION_SHORTCUTS:
        return PERMISSION_SHORTCUTS[permissions]
    # Try to parse as JSON
    try:
        return json.loads(permissions)
    except json.JSONDecodeError:
        valid = ", ".join(PERMISSION_SHORTCUTS)
        raise ValueError(f"Invalid --permissions value: {permissions!r}. Use a shortcut ({valid}) or a JSON object.")


def serve(host, port, backend_url, username=None, password=None, permissions=None, quota=None, socket_activation=False):
    backend = get_backend(backend_url, permissions=permissions, quota=quota)
    if backend is None:
        raise ValueError(f"Invalid backend URL: {backend_url}")
    if socket_activation:
        adopted = get_pre_bound_socket()
        adopted.setblocking(True)
        server_address = adopted.getsockname()
        server = BorgStoreRESTServer(server_address, backend, username, password, adopted_socket=adopted)
        logger.info(f"BorgStore REST server using systemd-activated socket on {server_address}")
    else:
        server = BorgStoreRESTServer((host, port), backend, username, password)
        logger.info(f"BorgStore REST server listening on {host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logger.setLevel(logging.INFO)
    parser = argparse.ArgumentParser(description="BorgStore REST Server")
    parser.add_argument(
        "--host", default="127.0.0.1", help="Address/hostname to listen on (ignored with --socket-activation)"
    )
    parser.add_argument("--port", type=int, default=5618, help="Port to listen on (ignored with --socket-activation)")
    parser.add_argument("--backend", required=True, help="Backend URL (e.g. file:///tmp/store)")
    parser.add_argument("--username", help="Basic Auth username")
    parser.add_argument("--password", help="Basic Auth password")
    parser.add_argument("--permissions", help="Permissions: a shortcut name or a JSON object string.")
    parser.add_argument("--quota", type=int, default=None, help="Quota in bytes.")
    parser.add_argument(
        "--socket-activation", action="store_true", help="Adopt pre-bound socket from systemd (SD_LISTEN_FDS)"
    )
    args = parser.parse_args()
    permissions = resolve_permissions(args.permissions)
    serve(
        args.host,
        args.port,
        args.backend,
        args.username,
        args.password,
        permissions,
        args.quota,
        args.socket_activation,
    )
