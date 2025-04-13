try:
    import boto3
    from botocore.client import Config
except ImportError:
    boto3 = None

import re
from typing import Optional
import urllib.parse

from borgstore.constants import TMP_SUFFIX

from ._base import BackendBase, ItemInfo, validate_name
from .errors import BackendError, BackendMustBeOpen, BackendMustNotBeOpen, BackendDoesNotExist, BackendAlreadyExists
from .errors import ObjectNotFound


def get_s3_backend(url: str):
    if boto3 is None:
        return None

    # (s3|b2):[profile|(access_key_id:access_key_secret)@][schema://hostname[:port]]/bucket/path
    s3_regex = r"""
        (s3|b2):
        ((
            (?P<profile>[^@:]+)  # profile (no colons allowed)
            |
            (?P<access_key_id>[^:@]+):(?P<access_key_secret>[^@]+)  # access key and secret
        )@)?  # optional authentication
        (?P<schema>[^:/]+)://
        (?P<hostname>[^:/]+)
        (:(?P<port>\d+))?/
        (?P<bucket>[^/]+)/  # bucket name
        (?P<path>.+)  # path
    """
    m = re.match(s3_regex, url, re.VERBOSE)
    if m:
        s3type = "s3" if url.find("s3:") == 0 else "b2"
        profile = m["profile"]
        access_key_id = m["access_key_id"]
        access_key_secret = m["access_key_secret"]
        if profile is not None and access_key_id is not None:
            raise BackendError("S3: profile and access_key_id cannot be specified at the same time")
        if access_key_id is not None and access_key_secret is None:
            raise BackendError("S3: access_key_secret is mandatory when access_key_id is specified")
        if access_key_id is not None:
            access_key_id = urllib.parse.unquote(access_key_id)
        if access_key_secret is not None:
            access_key_secret = urllib.parse.unquote(access_key_secret)
        schema = m["schema"]
        hostname = m["hostname"]
        port = m["port"]
        bucket = m["bucket"]
        path = m["path"]

        endpoint_url = None
        if schema and hostname:
            endpoint_url = f"{schema}://{hostname}"
            if port:
                endpoint_url += f":{port}"
        return S3(bucket=bucket, path=path, is_b2=s3type == "b2", profile=profile,
                  access_key_id=access_key_id, access_key_secret=access_key_secret,
                  endpoint_url=endpoint_url)


class S3(BackendBase):
    def __init__(self, bucket: str, path: str, is_b2: bool, profile: Optional[str] = None,
                 access_key_id: Optional[str] = None, access_key_secret: Optional[str] = None,
                 endpoint_url: Optional[str] = None):
        self.delimiter = '/'
        self.bucket = bucket
        self.base_path = path.rstrip(self.delimiter) + self.delimiter  # Ensure it ends with '/'
        self.opened = False
        if profile:
            session = boto3.Session(profile_name=profile)
        elif access_key_id and access_key_secret:
            session = boto3.Session(aws_access_key_id=access_key_id, aws_secret_access_key=access_key_secret)
        else:
            session = boto3.Session()
        config = None
        if is_b2:
            config = Config(
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            )
        self.s3 = session.client("s3", endpoint_url=endpoint_url, config=config)
        if is_b2:
            event_system = self.s3.meta.events
            event_system.register_first('before-sign.*.*', self._fix_headers)

    def _fix_headers(self, request, **kwargs):
        if "x-amz-checksum-crc32" in request.headers:
            del request.headers["x-amz-checksum-crc32"]
        if "x-amz-sdk-checksum-algorithm" in request.headers:
            del request.headers["x-amz-sdk-checksum-algorithm"]

    def _mkdir(self, name):
        try:
            key = (self.base_path + name).rstrip(self.delimiter) + self.delimiter
            self.s3.put_object(Bucket=self.bucket, Key=key)
        except self.s3.exceptions.ClientError as e:
            raise BackendError(f"S3 error: {e}")

    def create(self):
        if self.opened:
            raise BackendMustNotBeOpen()
        try:
            objects = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=self.base_path,
                                              Delimiter=self.delimiter, MaxKeys=1)
            if objects["KeyCount"] > 0:
                raise BackendAlreadyExists(f"Backend already exists: {self.base_path}")
            self._mkdir("")
        except self.s3.exceptions.NoSuchBucket:
            raise BackendDoesNotExist(f"S3 bucket does not exist: {self.bucket}")
        except self.s3.exceptions.ClientError as e:
            raise BackendError(f"S3 error: {e}")

    def destroy(self):
        if self.opened:
            raise BackendMustNotBeOpen()
        try:
            objects = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=self.base_path,
                                              Delimiter=self.delimiter, MaxKeys=1)
            if objects["KeyCount"] == 0:
                raise BackendDoesNotExist(f"Backend does not exist: {self.base_path}")
            is_truncated = True
            while is_truncated:
                objects = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=self.base_path, MaxKeys=1000)
                is_truncated = objects['IsTruncated']
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
            if size is None and offset == 0:
                obj = self.s3.get_object(Bucket=self.bucket, Key=key)
                return obj["Body"].read()
            elif size is not None and offset == 0:
                obj = self.s3.get_object(Bucket=self.bucket, Key=key, Range=f"bytes=0-{size - 1}")
                return obj["Body"].read()
            elif size is None and offset != 0:
                head = self.s3.head_object(Bucket=self.bucket, Key=key)
                length = head["ContentLength"]
                obj = self.s3.get_object(Bucket=self.bucket, Key=key, Range=f"bytes={offset}-{length - 1}")
                return obj["Body"].read()
            elif size is not None and offset != 0:
                obj = self.s3.get_object(Bucket=self.bucket, Key=key, Range=f"bytes={offset}-{offset + size - 1}")
                return obj["Body"].read()
        except self.s3.exceptions.NoSuchKey:
            raise ObjectNotFound(name)

    def delete(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        key = self.base_path + name
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            self.s3.delete_object(Bucket=self.bucket, Key=key)
        except self.s3.exceptions.NoSuchKey:
            raise ObjectNotFound(name)
        except self.s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
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
        base_prefix = (self.base_path + name).rstrip(self.delimiter) + self.delimiter
        try:
            start_after = ''
            is_truncated = True
            while is_truncated:
                objects = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=base_prefix,
                                                  Delimiter=self.delimiter, MaxKeys=1000, StartAfter=start_after)
                if objects['KeyCount'] == 0:
                    raise ObjectNotFound(name)
                is_truncated = objects["IsTruncated"]
                for obj in objects.get("Contents", []):
                    obj_name = obj["Key"][len(base_prefix):]  # Remove base_path prefix
                    if obj_name == "":
                        continue
                    if obj_name.endswith(TMP_SUFFIX):
                        continue
                    start_after = obj["Key"]
                    yield ItemInfo(name=obj_name, exists=True, size=obj["Size"], directory=False)
                for prefix in objects.get("CommonPrefixes", []):
                    dir_name = prefix["Prefix"][len(base_prefix):-1]  # Remove base_path prefix and trailing slash
                    yield ItemInfo(name=dir_name, exists=True, size=0, directory=True)
        except self.s3.exceptions.ClientError as e:
            raise BackendError(f"S3 error: {e}")

    def mkdir(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        self._mkdir(name)

    def rmdir(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        prefix = self.base_path + name.rstrip(self.delimiter) + self.delimiter
        objects = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix, Delimiter=self.delimiter, MaxKeys=2)
        if "Contents" in objects and len(objects["Contents"]) > 1:
            raise BackendError(f"Directory not empty: {name}")
        self.s3.delete_object(Bucket=self.bucket, Key=prefix)

    def info(self, name):
        if not self.opened:
            raise BackendMustBeOpen()
        validate_name(name)
        key = self.base_path + name
        try:
            obj = self.s3.head_object(Bucket=self.bucket, Key=key)
            return ItemInfo(name=name, exists=True, directory=False, size=obj["ContentLength"])
        except self.s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                try:
                    self.s3.head_object(Bucket=self.bucket, Key=key + self.delimiter)
                    return ItemInfo(name=name, exists=True, directory=True, size=0)
                except self.s3.exceptions.ClientError:
                    pass
                return ItemInfo(name=name, exists=False, directory=False, size=0)
            raise BackendError(f"S3 error: {e}")
