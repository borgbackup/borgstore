Backends
========

The backend API is rather simple; one only needs to provide some very
basic operations.

Existing backends are listed below; more might come in the future.

posixfs
-------

Use storage on a local POSIX filesystem:

- URL: ``file:///absolute/path``
- It is the caller's responsibility to convert a relative path into an absolute
  filesystem path.
- Namespaces: directories
- Values: in key-named files
- Quota: tracks backend storage size and rejects ``store`` if quota is exceeded.

  The current usage is persisted to a hidden file in the storage directory.

  When quota tracking is enabled on a backend that already contains data,
  the server automatically scans the directories at ``open`` time (that may
  take a while if there are many files). That scan can be avoided by always
  using quotas.
- Permissions: This backend can enforce a simple, test-friendly permission system
  and raises ``PermissionDenied`` if access is not permitted by the configuration.

  You provide a mapping of names (paths) to granted permission letters. Permissions
  apply to the exact name and all of its descendants (inheritance). If a name is not
  present in the mapping, its nearest ancestor is consulted, up to the empty name
  "" (the store root). If no mapping is provided at all, all operations are allowed.

  Permission letters:

  - ``l``: allow listing object names (directory/namespace listing)
  - ``r``: allow reading objects (contents)
  - ``w``: allow writing new objects (must not already exist)
  - ``W``: allow writing objects including overwriting existing objects
  - ``D``: allow deleting objects

  Operation requirements:

  - create(): requires ``w`` or ``W`` on the store root (``wW``)
  - destroy(): requires ``D`` on the store root
  - mkdir(name): requires ``w``
  - rmdir(name): requires ``w`` or ``D`` (``wD``)
  - list(name): requires ``l``
  - info(name): requires ``l`` (``r`` also accepted)
  - load(name): requires ``r``
  - store(name, value): requires ``w`` for new objects, ``W`` for overwrites (``wW``)
  - delete(name): requires ``D``
  - move(src, dst): requires ``D`` for the source and ``w``/``W`` for the destination

  Examples:

  - Read-only store (recursively): ``permissions = {"": "lr"}``
  - No-delete, no-overwrite (but allow adding new items): ``permissions = {"": "lrw"}``
  - Hierarchical rules: only allow listing at root, allow read/write in "dir",
    but only read for "dir/file":

    ::

        permissions = {
            "": "l",
            "dir": "lrw",
            "dir/file": "r",
        }

  To use permissions with ``Store`` and ``posixfs``, pass the mapping to Store and it
  will be handed to the posixfs backend:

  ::

      from borgstore import Store
      store = Store(url="file:///abs/path", permissions={"": "lrwWD"})
      store.create()
      store.open()
      # ...
      store.close()

sftp
----

Use storage on an SFTP server:

- URL: ``sftp://user@server:port/relative/path`` (strongly recommended)

  For users' and admins' convenience, the mapping of the URL path to the server filesystem path
  depends on the server configuration (home directory, sshd/sftpd config, ...).
  Usually the path is relative to the user's home directory.
- URL: ``sftp://user@server:port//absolute/path``

  As this uses an absolute path, some things become more difficult:

  - A user's configuration might break if a server admin moves a user's home to a new location.
  - Users must know the full absolute path of the space they are permitted to use.
- Namespaces: directories
- Values: in key-named files
- hash: runs the hexdigest computation server-side (if server supports check-file).

rclone
------

Use storage on any of the many cloud providers `rclone <https://rclone.org/>`_ supports:

- URL: ``rclone:remote:path`` — we just prefix "rclone:" and pass everything to the right
  of that to rclone; see: https://rclone.org/docs/#syntax-of-remote-paths
- The implementation primarily depends on the specific remote.
- The rclone binary path can be set via the environment variable ``RCLONE_BINARY`` (default: "rclone").


s3
--

Use storage on an S3-compliant cloud service:

- URL: ``(s3|b2):[profile|(access_key_id:access_key_secret)@][scheme://hostname[:port]]/bucket/path``

  The underlying backend is based on ``boto3``, so all standard boto3 authentication methods are supported:

  - provide a named profile (from your boto3 config),
  - include access key ID and secret in the URL,
  - or use default credentials (e.g., environment variables, IAM roles, etc.).

  See the `boto3 credentials documentation <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html>`_ for more details.

  If you're connecting to **AWS S3**, the ``[schema://hostname[:port]]`` part is optional.
  Bucket and path are always required.

  .. note::

     There is a known issue with some S3-compatible services (e.g., **Backblaze B2**).
     If you encounter problems, try using ``b2:`` instead of ``s3:`` in the URL.

- Namespaces: directories
- Values: in key-named files


REST (http/https)
-----------------

Use a storage backend running inside a BorgStore REST server process:

- URL: ``http[s]://[user:password@]host:port/path``
- Namespaces: depends on backend used by the server
- Values: depends on backend used by the server
- Authentication: Optional Basic Auth is supported.
- hash: runs the hexdigest computation server-side.
- defrag: runs the defragmentation helper server-side.
