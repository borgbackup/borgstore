import hashlib
import argparse
import json
import base64
import logging
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

    def _log(self, format, args, level=logging.INFO):
        addr = self.address_string()
        dt = self.log_date_time_string()
        user = self.server.username or "-"
        request_details = format % args
        logger.log(level, "%s %s %s [%s] %s" % (addr, "-", user, dt, request_details))

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
            authorized = username == self.server.username and password == self.server.password
            return authorized
        except Exception:
            logger.exception("Authentication code crashed, returning: unauthorized.")
            return False

    def respond(self, status=HTTP.OK, data=None, content_type=None, headers=None):
        self.send_response(status)
        if content_type:
            self.send_header("Content-Type", content_type)
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
        if isinstance(e, ObjectNotFound):
            self.send_error(HTTP.NOT_FOUND, str(e))
        elif isinstance(e, BackendDoesNotExist):
            self.send_error(HTTP.GONE, str(e))
        elif isinstance(e, BackendAlreadyExists):
            self.send_error(HTTP.CONFLICT, str(e))
        elif isinstance(e, (BackendMustBeOpen, BackendMustNotBeOpen)):
            self.send_error(HTTP.PRECONDITION_FAILED, str(e))
        elif isinstance(e, PermissionDenied):
            self.send_error(HTTP.FORBIDDEN, str(e))
        elif isinstance(e, QuotaExceeded):
            self.send_error(HTTP.INSUFFICIENT_STORAGE, str(e))
        elif isinstance(e, (ValueError, TypeError)):
            self.send_error(HTTP.BAD_REQUEST, str(e))
            logger.exception("Exception for %s", name or self.path)
        elif isinstance(e, BackendError):
            self.send_error(HTTP.INTERNAL_SERVER_ERROR, str(e))
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


class BorgStoreRESTServer(ThreadingHTTPServer):
    disable_nagle_algorithm = True  # aka TCP_NODELAY, reduces latency

    def __init__(self, server_address, backend, username=None, password=None):
        self.backend = backend
        self.username = username
        self.password = password
        super().__init__(server_address, BorgStoreRESTRequestHandler)


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


def serve(host, port, backend_url, username=None, password=None, permissions=None, quota=None):
    backend = get_backend(backend_url, permissions=permissions, quota=quota)
    if backend is None:
        raise ValueError(f"Invalid backend URL: {backend_url}")
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
    parser.add_argument("--host", default="127.0.0.1", help="Address/hostname to listen on")
    parser.add_argument("--port", type=int, default=5618, help="Port to listen on (default: 5618)")
    parser.add_argument("--backend", required=True, help="Backend URL (e.g. file:///tmp/store)")
    parser.add_argument("--username", help="Basic Auth username")
    parser.add_argument("--password", help="Basic Auth password")
    parser.add_argument("--permissions", help="Permissions: a shortcut name or a JSON object string.")
    parser.add_argument("--quota", type=int, default=None, help="Quota in bytes.")
    args = parser.parse_args()
    permissions = resolve_permissions(args.permissions)
    serve(args.host, args.port, args.backend, args.username, args.password, permissions, args.quota)
