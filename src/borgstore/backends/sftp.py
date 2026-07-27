"""
SFTP-based backend implementation — on an SFTP server, uses files in directories below a base path.
"""

from pathlib import Path
from urllib.parse import unquote
import errno
import functools
import logging
import random
import re
import socket
import stat
import time
from typing import Optional

try:
    import paramiko
except ImportError:
    paramiko = None

from ._base import BackendBase, ItemInfo, validate_name
from .errors import BackendError, BackendMustBeOpen, BackendMustNotBeOpen, BackendDoesNotExist, BackendAlreadyExists
from .errors import ObjectNotFound
from ..constants import TMP_SUFFIX
from ..utils import hashing

logger = logging.getLogger(__name__)

# How long to wait (seconds) for the initial connection / authentication to complete.
DEFAULT_CONNECT_TIMEOUT = 30.0
# Send an SSH keepalive every N seconds. This keeps NAT/firewall state alive and, together with
# the socket timeout below, helps notice a dead connection reasonably quickly.
DEFAULT_KEEPALIVE_INTERVAL = 30.0
# If no data is received on the SFTP channel for this long, the operation raises socket.timeout
# instead of blocking forever. This is what turns a "dead" connection (e.g. after the laptop was
# suspended and the network went away) into a recoverable error instead of an infinite hang.
DEFAULT_SOCKET_TIMEOUT = 120.0
# When the connection was lost, try this many times to reconnect and redo the failed operation.
DEFAULT_RECONNECT_TRIES = 3
# Wait this long (seconds) between reconnect attempts.
DEFAULT_RECONNECT_WAIT = 5.0

# errnos that mean "the connection is gone" (as opposed to e.g. ENOENT / EACCES which are legit results).
_CONNECTION_LOST_ERRNOS = frozenset(
    e
    for e in (
        getattr(errno, name, None)
        for name in (
            "EPIPE",
            "ECONNRESET",
            "ECONNABORTED",
            "ENETDOWN",
            "ENETRESET",
            "ENETUNREACH",
            "EHOSTDOWN",
            "EHOSTUNREACH",
            "ETIMEDOUT",
            "ESHUTDOWN",
        )
    )
    if e is not None
)


def _is_connection_lost(exc: BaseException) -> bool:
    """Return True if exc indicates a broken/dead connection (something a reconnect could fix)."""
    if isinstance(exc, (EOFError, socket.timeout, ConnectionError)):
        # socket.timeout is our own doing (see DEFAULT_SOCKET_TIMEOUT); EOFError is raised by
        # paramiko when the server connection dropped; ConnectionError covers reset/aborted/refused.
        return True
    if paramiko is not None and isinstance(exc, paramiko.SSHException):
        # transport/protocol level failures, e.g. "Server connection dropped".
        return True
    if isinstance(exc, OSError) and exc.errno in _CONNECTION_LOST_ERRNOS:
        # low-level socket errors. Note: FileNotFoundError/PermissionError are OSError subclasses too,
        # but their errnos (ENOENT/EACCES) are not in the set, so they are correctly treated as real results.
        return True
    return False


def with_reconnect(method=None, *, swallow_not_found=False):
    """Decorator: if the wrapped method fails because the connection was lost, reconnect and retry.

    This is only active while the backend is opened. Errors that are not connection losses
    (e.g. ObjectNotFound, FileNotFoundError, PermissionError) are passed through unchanged.

    Usable both bare (``@with_reconnect``) and with arguments (``@with_reconnect(...)``).

    swallow_not_found: only for idempotent removals (delete/move). The retry path is only reached
    after a connection loss on an earlier attempt, so if that earlier attempt had already succeeded
    (just the reply got lost), the retried operation raises ObjectNotFound. For delete/move that is
    a spurious error - the desired end state (object gone / moved) is reached - so we swallow it and
    report success. On the very first attempt ObjectNotFound is a real result and still propagates.
    """
    if method is None:
        return functools.partial(with_reconnect, swallow_not_found=swallow_not_found)

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except Exception as exc:
            if not (self.opened and _is_connection_lost(exc)):
                raise
            logger.warning("sftp: connection lost (%r), trying to reconnect...", exc)
        last_exc: Optional[BaseException] = None
        for attempt in range(1, self.reconnect_tries + 1):
            time.sleep(self.reconnect_wait)
            try:
                self._reconnect()
            except Exception as exc:
                last_exc = exc
                if not _is_connection_lost(exc):
                    raise
                logger.warning("sftp: reconnect attempt %d/%d failed: %r", attempt, self.reconnect_tries, exc)
                continue
            try:
                result = method(self, *args, **kwargs)
            except ObjectNotFound:
                if not swallow_not_found:
                    raise
                # an earlier attempt (before the connection loss) most likely already did it.
                logger.info("sftp: reconnected (attempt %d); object already gone, treating as success.", attempt)
                return None
            except Exception as exc:
                last_exc = exc
                if not _is_connection_lost(exc):
                    raise
                logger.warning(
                    "sftp: retry after reconnect (attempt %d/%d) failed: %r", attempt, self.reconnect_tries, exc
                )
                continue
            logger.info("sftp: reconnected successfully (attempt %d).", attempt)
            return result
        raise BackendError(f"sftp connection was lost and could not be reestablished: {last_exc!r}")

    return wrapper


def get_sftp_backend(url):
    """Get SFTP backend from URL."""

    if not url.startswith("sftp:"):
        return None

    if paramiko is None:
        raise BackendDoesNotExist(
            "The SFTP backend requires dependencies. Install them with: 'pip install borgstore[sftp]'"
        )

    # sftp://username@hostname:22/path
    # Notes:
    # - username and port are optional
    # - host must be a hostname (not an IP address)
    # - you must provide a path; by default it is a relative path (usually relative to the user's home directory —
    #   this allows the SFTP server admin to move things without the user needing to know).
    # - giving an absolute path is also possible: sftp://username@hostname:22//home/username/borgstore
    sftp_regex = r"""
        sftp://
        ((?P<username>[^@]+)@)?
        (?P<hostname>([^:/]+))(?::(?P<port>\d+))?/  # slash as separator, not part of the path
        (?P<path>(.+))  # path may or may not start with a slash, must not be empty
    """
    m = re.match(sftp_regex, url, re.VERBOSE)
    if m:
        return Sftp(
            username=unquote(m["username"]) if m["username"] else None,
            hostname=m["hostname"],
            port=int(m["port"] or "0"),
            path=unquote(m["path"]),
        )


class Sftp(BackendBase):
    """BorgStore backend for SFTP."""

    # Sftp implementation supports precreate = True as well as = False.
    precreate_dirs: bool = False

    def __init__(
        self,
        hostname: str,
        path: str,
        port: int = 0,
        username: Optional[str] = None,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        keepalive_interval: float = DEFAULT_KEEPALIVE_INTERVAL,
        socket_timeout: float = DEFAULT_SOCKET_TIMEOUT,
        reconnect_tries: int = DEFAULT_RECONNECT_TRIES,
        reconnect_wait: float = DEFAULT_RECONNECT_WAIT,
    ):
        self.username = username
        self.hostname = hostname
        self.port = port
        self.base_path = path
        self.connect_timeout = connect_timeout
        self.keepalive_interval = keepalive_interval
        self.socket_timeout = socket_timeout
        self.reconnect_tries = reconnect_tries
        self.reconnect_wait = reconnect_wait
        self.opened = False
        self.check_file_supported = True
        self.ssh: Optional[paramiko.SSHClient] = None
        self.client: Optional[paramiko.SFTPClient] = None
        if paramiko is None:
            raise BackendError("sftp backend unavailable: could not import paramiko!")

    def _get_host_config_from_file(self, path: str, hostname: str):
        """Look up the configuration for hostname in path (SSH config file)."""
        config_path = Path(path).expanduser()
        try:
            ssh_config = paramiko.SSHConfig.from_path(config_path)
        except FileNotFoundError:
            return paramiko.SSHConfigDict()  # empty dict
        else:
            return ssh_config.lookup(hostname)

    def _get_host_config(self):
        """Assemble all provided and configured host configuration values."""
        host_config = paramiko.SSHConfigDict()
        # self.hostname might be an alias/shortcut (with real hostname given in configuration),
        # but there might be also nothing in the configs at all for self.hostname:
        host_config["hostname"] = self.hostname
        # First process system-wide SSH config, then override with user SSH config:
        host_config.update(self._get_host_config_from_file("/etc/ssh/ssh_config", self.hostname))
        # Note: no support yet for /etc/ssh/ssh_config.d/*
        host_config.update(self._get_host_config_from_file("~/.ssh/config", self.hostname))
        # Now override configured values with provided values
        if self.username is not None:
            host_config["user"] = self.username
        if self.port != 0:
            host_config["port"] = str(self.port)
        # Make sure port is present.
        host_config["port"] = str(host_config.get("port") or "22")
        return host_config

    def _connect(self):
        try:
            self.ssh = paramiko.SSHClient()
            # Note: we do not deal with unknown hosts and ssh.set_missing_host_key_policy here.
            # The user should make the first contact to any new host using the ssh or sftp CLI command
            # and interactively verify remote host fingerprints.
            self.ssh.load_system_host_keys()  # This is documented to load the user's known_hosts file
            host_config = self._get_host_config()
            self.ssh.connect(
                hostname=host_config["hostname"],
                username=host_config.get("user"),  # if None, paramiko will use current user
                port=int(host_config["port"]),
                key_filename=host_config.get("identityfile"),  # list of keys, ~ is already expanded
                allow_agent=True,
                # bound the time we wait to establish the connection / authenticate, so a dead
                # network does not make us block forever already at connect time.
                timeout=self.connect_timeout,
                banner_timeout=self.connect_timeout,
                auth_timeout=self.connect_timeout,
            )
            transport = self.ssh.get_transport()
            if transport is not None and self.keepalive_interval:
                # keep NAT/firewall state alive and notice a dead peer sooner.
                transport.set_keepalive(int(self.keepalive_interval))
            self.client = self.ssh.open_sftp()
            if self.socket_timeout:
                # crucial: without a timeout, an operation on a dead connection (e.g. after the
                # machine was suspended and the network went away) would block forever. With a
                # timeout it raises socket.timeout, which we turn into a reconnect (see with_reconnect).
                channel = self.client.get_channel()
                if channel is not None:
                    channel.settimeout(self.socket_timeout)
        except Exception:
            self._disconnect()
            raise

    def _disconnect(self):
        # be robust: closing a already-dead connection may itself raise, but we still want to
        # drop our references so a following _connect starts from a clean slate.
        try:
            if self.client:
                self.client.close()
        except Exception:
            pass
        finally:
            self.client = None
        try:
            if self.ssh:
                self.ssh.close()
        except Exception:
            pass
        finally:
            self.ssh = None

    def _reconnect(self):
        """Drop a (likely broken) connection and establish a fresh, working one."""
        self._disconnect()
        self._connect()
        # .open() changed the working directory into base_path and all operations use relative
        # names, so we must restore that after reconnecting.
        self.client.chdir(self.base_path)

    def create(self):
        if self.opened:
            raise BackendMustNotBeOpen()
        self._connect()
        try:
            # We accept an already existing empty directory and we also optionally create
            # any missing parent dirs. The latter is important for repository hosters that
            # only offer limited access to their storage (e.g., only via borg/borgstore).
            # It is also simpler than requiring users to create parent dirs separately.
            self._mkdir(self.base_path, exist_ok=True, parents=True)
            # Prevent users from creating a mess by using non-empty directories:
            contents = list(self.client.listdir(self.base_path))
            if contents:
                raise BackendAlreadyExists(f"sftp storage base path is not empty: {self.base_path}")
        except IOError as err:
            raise BackendError(f"sftp storage I/O error: {err}")
        finally:
            self._disconnect()

    def destroy(self):
        def delete_recursive(path):
            parent = Path(path)
            for child_st in self.client.listdir_attr(str(parent)):
                child = parent / child_st.filename
                if stat.S_ISDIR(child_st.st_mode):
                    delete_recursive(child)
                else:
                    self.client.unlink(str(child))
            try:
                self.client.rmdir(str(parent))
            except OSError as e:
                # usually, this is because of missing permissions.
                if path != self.base_path:
                    raise e from None
                # do not raise if we can't remove the base path directory.
                # .create accepts an already existing base path, thus
                # .destroy may leave an existing base path behind.

        if self.opened:
            raise BackendMustNotBeOpen()
        self._connect()
        try:
            try:
                self.client.stat(self.base_path)  # check if this storage exists, fail early if not.
            except FileNotFoundError:
                raise BackendDoesNotExist(f"sftp storage base path does not exist: {self.base_path}") from None
            delete_recursive(self.base_path)
        finally:
            self._disconnect()

    def open(self):
        if self.opened:
            raise BackendMustNotBeOpen()
        self._connect()
        try:
            st = self.client.stat(self.base_path)  # check if this storage exists, fail early if not.
        except FileNotFoundError:
            raise BackendDoesNotExist(f"sftp storage base path does not exist: {self.base_path}") from None
        if not stat.S_ISDIR(st.st_mode):
            raise BackendDoesNotExist(f"sftp storage base path is not a directory: {self.base_path}")
        self.client.chdir(self.base_path)  # this sets the cwd we work in!
        self.opened = True

    def close(self):
        if not self.opened:
            raise BackendMustBeOpen()
        self._disconnect()
        self.opened = False

    def _mkdir(self, name, *, parents=False, exist_ok=False):
        # Path.mkdir, but via sftp
        p = Path(name)
        try:
            self.client.mkdir(str(p))
        except FileNotFoundError:
            # the parent dir is missing
            if not parents:
                raise
            # first create parent dir(s), recursively:
            self._mkdir(p.parents[0], parents=parents, exist_ok=exist_ok)
            # then retry:
            self.client.mkdir(str(p))
        except OSError:
            # maybe p already existed?
            if not exist_ok:
                raise

    @with_reconnect
    def mkdir(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        self._mkdir(name, parents=True, exist_ok=True)

    @with_reconnect
    def rmdir(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        try:
            self.client.rmdir(name)
        except FileNotFoundError:
            raise ObjectNotFound(name) from None

    @with_reconnect
    def info(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        try:
            st = self.client.stat(name)
        except FileNotFoundError:
            return ItemInfo(name=name, exists=False, directory=False, size=0)
        else:
            is_dir = stat.S_ISDIR(st.st_mode)
            return ItemInfo(name=name, exists=True, directory=is_dir, size=st.st_size)

    @with_reconnect
    def load(self, name, *, size=None, offset=0):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        try:
            with self.client.open(name) as f:
                f.seek(offset, 0 if offset >= 0 else 2)
                f.prefetch(size)  # speeds up the following read() significantly!
                return f.read(size)
        except FileNotFoundError:
            raise ObjectNotFound(name) from None

    @with_reconnect
    def store(self, name, value):
        def _write_to_tmpfile():
            with self.client.open(tmp_name, mode="w") as f:
                f.set_pipelined(True)  # speeds up the following write() significantly!
                f.write(value)

        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        tmp_dir = Path(name).parent
        # write to a differently named temp file in same directory first,
        # so the store never sees partially written data.
        tmp_name = str(tmp_dir / ("".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=8)) + TMP_SUFFIX))
        try:
            # try to do it quickly, not doing the mkdir. each sftp op might be slow due to latency.
            # this will frequently succeed, because the dir is already there.
            _write_to_tmpfile()
        except FileNotFoundError:
            # retry, create potentially missing dirs first. this covers these cases:
            # - either the dirs were not precreated
            # - a previously existing directory was "lost" in the filesystem
            self._mkdir(str(tmp_dir), parents=True, exist_ok=True)
            _write_to_tmpfile()
        # rename it to the final name:
        try:
            self.client.posix_rename(tmp_name, name)
        except OSError:
            self.client.unlink(tmp_name)
            raise

    @with_reconnect(swallow_not_found=True)
    def delete(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        try:
            self.client.unlink(name)
        except FileNotFoundError:
            raise ObjectNotFound(name) from None

    def _sftp_hash(self, name: str, algorithm: str) -> str | None:
        # Sadly, as of 2026-03-28, this is not supported by OpenSSH,
        # but by some less popular SFTP servers.
        if algorithm == hashing.BLAKE3:
            # the check-file extension only defines md5 / sha* algorithms. asking for blake3
            # would just fail and, worse, make us give up on check-file for all later calls.
            return None
        if self.check_file_supported:
            try:
                with self.client.open(name) as f:
                    digest = f.check(algorithm)
                    return digest.hex()
            except FileNotFoundError:
                raise ObjectNotFound(name) from None
            except IOError:
                # check-file not supported or algorithm not supported
                self.check_file_supported = False
        return None

    @with_reconnect
    def hash(self, name: str, algorithm: str = "sha256") -> str:
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        hexdigest = self._sftp_hash(name, algorithm)
        if hexdigest is not None:
            return hexdigest
        return super().hash(name, algorithm=algorithm)

    @with_reconnect(swallow_not_found=True)
    def move(self, curr_name, new_name):
        def _rename_to_new_name():
            self.client.posix_rename(curr_name, new_name)

        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(curr_name)
        validate_name(new_name)
        parent_dir = Path(new_name).parent
        try:
            # try to do it quickly, not doing the mkdir. each sftp op might be slow due to latency.
            # this will frequently succeed, because the dir is already there.
            _rename_to_new_name()
        except FileNotFoundError:
            # retry, create potentially missing dirs first. this covers these cases:
            # - either the dirs were not precreated
            # - a previously existing directory was "lost" in the filesystem
            self._mkdir(str(parent_dir), parents=True, exist_ok=True)
            try:
                _rename_to_new_name()
            except FileNotFoundError:
                raise ObjectNotFound(curr_name) from None

    @with_reconnect
    def _listdir_attr(self, name):
        # separate, decorated helper because .list() is a generator: connection errors would be
        # raised while iterating, i.e. outside the reconnect wrapper. Fetching the whole listing
        # here means the (retryable) network access happens inside with_reconnect.
        try:
            return self.client.listdir_attr(name)
        except FileNotFoundError:
            raise ObjectNotFound(name) from None

    def list(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        infos = self._listdir_attr(name)
        for info in sorted(infos, key=lambda i: i.filename):
            try:
                validate_name(info.filename)
            except ValueError:
                pass  # that file is likely not from us or is still uploading
            else:
                is_dir = stat.S_ISDIR(info.st_mode)
                yield ItemInfo(name=info.filename, exists=True, size=info.st_size, directory=is_dir)
