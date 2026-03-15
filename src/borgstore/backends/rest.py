"""
REST http client based backend implementation (use with borgstore.server.rest).
"""

import os
import re
from typing import Iterator, Dict, Optional
from http import HTTPStatus as HTTP
from urllib.parse import unquote

try:
    import requests
    from requests.auth import HTTPBasicAuth
except ImportError:
    requests = HTTPBasicAuth = None

from ._base import BackendBase, ItemInfo, validate_name
from .errors import (
    ObjectNotFound,
    BackendAlreadyExists,
    BackendDoesNotExist,
    PermissionDenied,
    BackendError,
    BackendMustBeOpen,
    BackendMustNotBeOpen,
)


def get_rest_backend(base_url: str):
    # http(s)://username:password@hostname:port/ or http(s)://hostname:port/ + auth from env
    # note: path component must be "/" (no sub-path allowed, as it would silently prepend to all item names)
    if not base_url.startswith(("http:", "https:")):
        return None

    if requests is None:
        raise BackendDoesNotExist(
            "The REST backend requires dependencies. Install them with: 'pip install borgstore[rest]'"
        )

    http_regex = r"""
        (?P<scheme>http|https)://
        ((?P<username>[^:]+):(?P<password>[^@]+)@)?
        (?P<host>[^:/]+)(:(?P<port>\d+))?
        (?P<path>/)
    """
    m = re.match(http_regex, base_url, re.VERBOSE)
    if m:
        scheme = m.group("scheme")
        host = m.group("host")
        port = m.group("port")
        path = m.group("path")

        base_url = f"{scheme}://{host}{f':{port}' if port else ''}{path}"

        username, password = m.group("username"), m.group("password")
        if username and password:
            username, password = unquote(username), unquote(password)
        else:
            username, password = os.environ.get("BORGSTORE_REST_USERNAME"), os.environ.get("BORGSTORE_REST_PASSWORD")

        return REST(base_url, username=username, password=password)


class REST(BackendBase):
    def __init__(
        self,
        base_url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = 30,
    ):
        self.base_url = base_url.rstrip("/")  # _url method adds slash
        self.headers = headers or {}
        self.headers["Accept"] = "application/vnd.x.borgstore.rest.v1"
        self.timeout = timeout
        self.auth = HTTPBasicAuth(username, password) if username and password else None
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

        r_hdr = (None if not offset else f"bytes={offset}-") if size is None else f"bytes={offset}-{offset + size - 1}"
        headers = self.headers.copy()
        if r_hdr:
            headers["Range"] = r_hdr

        response = self._request("get", self._url(name), headers=headers)
        self._handle_response(response, name)
        return response.content

    def store(self, name: str, value: bytes) -> None:
        self._assert_open()
        validate_name(name)
        response = self._request("post", self._url(name), data=value)
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

    def list(self, name: str) -> Iterator[ItemInfo]:
        self._assert_open()
        validate_name(name)
        response = self._request("get", self._url(name) + "/")  # trailing "/" needed to get list
        self._handle_response(response, name)
        for entry in response.json():
            yield ItemInfo(name=entry["name"], exists=True, size=entry["size"], directory=entry.get("directory", False))
