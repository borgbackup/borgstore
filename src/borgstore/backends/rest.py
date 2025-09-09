"""
REST http client based backend implementation.

Usage:

b = get_rest_backend("https://username:password@username.repo.borgbase.com/restictest")
b.open()
b.create()
b.store("config", b"foo")
b.load("config")
b.delete("config")
b.store("config", b"bar")
b.store("data/<sha256(value)>", value)
b.list("data")
b.load("data/<sha256>")
b.close()
"""
import os
import re
import requests
from typing import Iterator, Dict, Optional
from urllib.parse import unquote

from requests.auth import HTTPBasicAuth

from ._base import BackendBase, ItemInfo, validate_name
from .errors import ObjectNotFound


def get_rest_backend(base_url: str):
    # http(s)://username:password@hostname:port/path or http(s)://hostname:port/path + auth from env
    http_regex = r"""
        (?P<scheme>http|https)://
        ((?P<username>[^:]+):(?P<password>[^@]+)@)?
        (?P<host>[^:/]+)(:?(?P<port>\d+))?
        (?P<path>(/.*))
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
            username, password = os.environ.get("REST_BACKEND_USERNAME"), os.environ.get("REST_BACKEND_PASSWORD")

        return RestClientBackend(base_url, username=username, password=password)


class RestClientBackend(BackendBase):
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
        self.headers["Accept"] = "application/vnd.x.restic.rest.v2"
        self.timeout = timeout
        self.auth = HTTPBasicAuth(username, password) if username and password else None
        self.session = None

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    def _request(self, method, url, *, headers=None, data=None, params=None):
        if self.session is not None:  # between .open() and .close()
            return self.session.request(method, url, params=params, data=data, headers=headers, timeout=self.timeout)
        else:  # .create() and .destroy() are called when backend is not opened
            assert headers is None
            return requests.request(
                method, url, auth=self.auth, params=params, data=data, headers=self.headers, timeout=self.timeout
            )

    def create(self) -> None:
        # restic-server: repo creation creates all needed directories
        response = self._request("post", self._url(""), params={"create": "true"})
        if response.status_code != 200:
            response.raise_for_status()

    def destroy(self) -> None:
        # XXX restic-server: repo deletion doesn't work on borgbase.com, 405 "Method not allowed"
        response = self._request("delete", self._url(""))
        if response.status_code != 200:
            response.raise_for_status()

    def open(self):
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update(self.headers)

    def close(self):
        if self.session is not None:
            self.session.close()
            self.session = None

    def mkdir(self, name: str) -> None:
        pass

    def rmdir(self, name: str) -> None:
        pass

    def info(self, name: str) -> ItemInfo:
        # restic-server: only works on objects, not on directories
        validate_name(name)
        response = self._request("head", self._url(name))
        if response.status_code != 200:
            if response.status_code == 404:
                raise ObjectNotFound(name)
            else:
                response.raise_for_status()
        return ItemInfo(name=name, exists=True, size=int(response.headers["Content-Length"]), directory=False)

    def load(self, name: str, *, size=None, offset=0) -> bytes:
        validate_name(name)

        r_hdr = (None if not offset else f"bytes={offset}-") if size is None else f"bytes={offset}-{offset + size - 1}"
        headers = self.headers.copy()
        if r_hdr:
            headers["Range"] = r_hdr

        response = self._request("get", self._url(name), headers=headers)
        if response.status_code != 200:
            if response.status_code == 404:
                raise ObjectNotFound(name)
            else:
                response.raise_for_status()
        return response.content

    def store(self, name: str, value: bytes) -> None:
        validate_name(name)
        # restic-server only works with key == sha256(value) (verifies the hash while writing to disk)
        # and it rejects overwriting existing objects.
        response = self._request("post", self._url(name), data=value)
        if response.status_code != 200:
            response.raise_for_status()

    def delete(self, name: str) -> None:
        validate_name(name)
        response = self._request("delete", self._url(name))
        if response.status_code != 200:
            if response.status_code == 404:
                raise ObjectNotFound(name)
            else:
                response.raise_for_status()

    def move(self, curr_name: str, new_name: str) -> None:
        raise NotImplementedError

    def list(self, name: str) -> Iterator[ItemInfo]:
        validate_name(name)
        response = self._request("get", self._url(name) + "/")  # trailing "/" needed to get list
        if response.status_code != 200:
            if response.status_code == 404:
                raise ObjectNotFound(name)
            else:
                response.raise_for_status()
        for entry in response.json():
            yield ItemInfo(name=entry["name"], exists=True, size=entry["size"], directory=False)
