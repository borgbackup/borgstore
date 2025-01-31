try:
    import boto3
except ImportError:
    boto3 = None

import random
import re
from pathlib import Path
from typing import Optional

from ._base import BackendBase, ItemInfo, validate_name
from .errors import BackendError, BackendMustBeOpen, BackendMustNotBeOpen, BackendDoesNotExist, BackendAlreadyExists
from .errors import ObjectNotFound
from ..constants import TMP_SUFFIX

def get_s3_backend(url):
    if boto3 is None:
        return None
    
    # s3:profile@hostname:port/bucket/path
    s3_regex = r"""
        s3:
        ((?P<profile>[^@]+)@)?  # optional profile
        (?P<hostname>[^:/]+)  # hostname
        (:(?P<port>\d+))?/  # optional port
        (?P<bucket>[^/]+)/  # bucket name
        (?P<path>.+)  # path
    """
    m = re.match(s3_regex, url, re.VERBOSE)
    if m:
        profile = m["profile"]
        hostname = m["hostname"]
        port = m["port"]
        bucket = m["bucket"]
        path = m["path"]

        endpoint_scheme = "https" if not port or port not in ("80", "8080") else "http"
        endpoint_url = f"{endpoint_scheme}://{hostname}:{port}" if port else f"{endpoint_scheme}://{hostname}"

        return S3(bucket=bucket, path=path, profile=profile, endpoint_url=endpoint_url)

class S3(BackendBase):
    def __init__(self, bucket: str, path: str, profile: Optional[str] = None, endpoint_url: Optional[str] = None):
        self.bucket = bucket
        self.base_path = path.rstrip('/') + '/'  # Ensure it ends with '/'
        self.opened = False
        session = boto3.Session(profile_name=profile) if profile else boto3.Session()
        self.s3 = session.client("s3", endpoint_url=endpoint_url)

    def create(self):
        if self.opened:
            raise BackendMustNotBeOpen()
        try:
            self.s3.head_bucket(Bucket=self.bucket)
            self.s3.put_object(Bucket=self.bucket, Key=self.base_path, Body=b"")
        except self.s3.exceptions.NoSuchBucket:
            raise BackendDoesNotExist(f"S3 bucket does not exist: {self.bucket}")
        except self.s3.exceptions.ClientError as e:
            raise BackendError(f"S3 error: {e}")

    def destroy(self):
        if self.opened:
            raise BackendMustNotBeOpen()
        try:
            objects = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=self.base_path)
            if "Contents" in objects:
                self.s3.delete_objects(
                    Bucket=self.bucket,
                    Delete={"Objects": [{"Key": obj["Key"]} for obj in objects["Contents"]]}
                )
        except self.s3.exceptions.ClientError as e:
            raise BackendError(f"S3 error: {e}")

    def open(self):
        if self.opened:
            raise BackendMustNotBeOpen()
        self.opened = True

    def close(self):
        if not self.opened:
            raise BackendMustBeOpen()
        self.opened = False

    def store(self, name, value):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        key = self.base_path + name
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=value)

    def load(self, name, *, size=None, offset=0):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        key = self.base_path + name
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            return obj["Body"].read()[offset:offset + (size or len(obj["Body"].read()))]
        except self.s3.exceptions.NoSuchKey:
            raise ObjectNotFound(name)

    def delete(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        key = self.base_path + name
        try:
            self.s3.delete_object(Bucket=self.bucket, Key=key)
        except self.s3.exceptions.NoSuchKey:
            raise ObjectNotFound(name)

    def move(self, curr_name, new_name):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(curr_name)
        validate_name(new_name)
        src_key = self.base_path + curr_name
        dest_key = self.base_path + new_name
        try:
            self.s3.copy_object(Bucket=self.bucket, CopySource={"Bucket": self.bucket, "Key": src_key}, Key=dest_key)
            self.s3.delete_object(Bucket=self.bucket, Key=src_key)
        except self.s3.exceptions.NoSuchKey:
            raise ObjectNotFound(curr_name)

    def list(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        prefix = self.base_path + name.rstrip('/') + '/'
        try:
            objects = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix, Delimiter='/')
            if "Contents" not in objects and "CommonPrefixes" not in objects:
                raise ObjectNotFound(name)
            for obj in objects.get("Contents", []):
                obj_name = obj["Key"][len(self.base_path):]  # Remove base_path prefix
                yield ItemInfo(name=obj_name, exists=True, size=obj["Size"], directory=False)
            for prefix in objects.get("CommonPrefixes", []):
                dir_name = prefix["Prefix"][len(self.base_path):-1]  # Remove base_path prefix and trailing slash
                yield ItemInfo(name=dir_name, exists=True, size=0, directory=True)
        except self.s3.exceptions.ClientError as e:
            raise BackendError(f"S3 error: {e}")

    def info(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        key = self.base_path + name
        try:
            obj = self.s3.head_object(Bucket=self.bucket, Key=key)
            return ItemInfo(name=name, exists=True, directory=False, size=obj["ContentLength"])
        except self.s3.exceptions.NoSuchKey:
            return ItemInfo(name=name, exists=False, directory=False, size=0)
