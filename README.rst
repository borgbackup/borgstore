BorgStore
=========

A key/value store implementation in Python, supporting multiple backends.

Keys
----

A key (str) can look like:

- 0123456789abcdef... (usually a long, hex-encoded hash value)
- Any other pure ASCII string without '/', '..', or spaces.


Namespaces
----------

To keep things separate, keys should be prefixed with a namespace, such as:

- config/settings
- meta/0123456789abcdef...
- data/0123456789abcdef...

Please note:

1. You should always use namespaces.
2. Nested namespaces like namespace1/namespace2/key are not supported.
3. The code can work without a namespace (empty namespace ""), but then you
   can't add another namespace later, because that would create
   nested namespaces.

Values
------

Values can be any arbitrary binary data (bytes).

Store Operations
----------------

The high-level Store API implementation transparently deals with nesting and
soft deletion, so the caller doesn't need to care much about that, and the backend
API can be much simpler:

- create/destroy: initialize or remove the whole store.
- list: flat list of the items in the given namespace (by default, only non-deleted
  items; optionally, only soft-deleted items).
- store: write a new item into the store (providing its key/value pair).
- load: read a value from the store (given its key); partial loads specifying
  an offset and/or size are supported.
- info: get information about an item via its key (exists, size, ...).
- delete: immediately remove an item from the store (given its key).
- move: implements renaming, soft delete/undelete, and moving to the current
  nesting level.
- stats: API call counters, time spent in API methods, data volume/throughput.
- latency/bandwidth emulator: can emulate higher latency (via BORGSTORE_LATENCY
  [us]) and lower bandwidth (via BORGSTORE_BANDWIDTH [bit/s]) than what is
  actually provided by the backend.

Store operations (and per-op timing and volume) are logged at DEBUG log level.

Automatic Nesting
-----------------

For the Store user, items have names such as:

- namespace/0123456789abcdef...
- namespace/abcdef0123456789...

If there are very many items in the namespace, this could lead to scalability
issues in the backend. The Store implementation therefore offers transparent
nesting, so that internally the backend API is called with names such as:

- namespace/01/23/56/0123456789abcdef...
- namespace/ab/cd/ef/abcdef0123456789...

The nesting depth can be configured from 0 (= no nesting) to N levels and
there can be different nesting configurations depending on the namespace.

The Store supports operating at different nesting levels in the same
namespace at the same time.

When using nesting depth > 0, the backends assume that keys are hashes
(contain hex digits) because some backends pre-create the nesting
directories at initialization time to optimize backend performance.

Soft deletion
-------------

To soft-delete an item (so its value can still be read or it can be
undeleted), the store just renames the item, appending ".del" to its name.

Undelete reverses this by removing the ".del" suffix from the name.

Some store operations provide a boolean flag "deleted" to control whether they
consider soft-deleted items.

Backends
--------

The backend API is rather simple; one only needs to provide some very
basic operations.

Existing backends are listed below; more might come in the future.

posixfs
~~~~~~~

Use storage on a local POSIX filesystem:

- URL: ``file:///absolute/path``
- It is the caller's responsibility to convert a relative path into an absolute
  filesystem path.
- Namespaces: directories
- Values: in key-named files
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
~~~~

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

rclone
~~~~~~

Use storage on any of the many cloud providers `rclone <https://rclone.org/>`_ supports:

- URL: ``rclone:remote:path`` — we just prefix "rclone:" and pass everything to the right
  of that to rclone; see: https://rclone.org/docs/#syntax-of-remote-paths
- The implementation primarily depends on the specific remote.
- The rclone binary path can be set via the environment variable ``RCLONE_BINARY`` (default: "rclone").


s3
~~

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


Scalability
-----------

- Count of key/value pairs stored in a namespace: automatic nesting is
  provided for keys to address common scalability issues.
- Key size: there are no special provisions for extremely long keys (e.g.,
  exceeding backend limitations). Usually this is not a problem, though.
- Value size: there are no special provisions for dealing with large value
  sizes (e.g., more than available memory, more than backend storage limitations,
  etc.). If one deals with very large values, one usually cuts them into
  chunks before storing them in the store.
- Partial loads improve performance by avoiding a full load if only part
  of the value is needed (e.g., a header with metadata).

Installation
------------

Install without the ``sftp:`` or ``s3:`` backend::

    pip install borgstore
    pip install "borgstore[none]"  # same thing (simplifies automation)

Install with the ``sftp:`` backend (more dependencies)::

    pip install "borgstore[sftp]"

Install with the ``s3:`` backend (more dependencies)::

    pip install "borgstore[s3]"

Please note that ``rclone:`` also supports SFTP and S3 remotes.

Want a demo?
------------

Run this to get instructions on how to run the demo::

    python3 -m borgstore

State of this project
---------------------

**API is still unstable and expected to change as development goes on.**

**As long as the API is unstable, there will be no data migration tools,
such as tools for upgrading an existing store's data to a new release.**

There are tests, and they pass for the basic functionality, so some functionality is already working well.

There might be missing features or optimization potential. Feedback is welcome!

Many possible backends are still missing. If you want to create and support one, pull requests are welcome.

Borg?
-----

Please note that this code is currently **not** used by the stable release of
BorgBackup (also known as "borg"), but only by Borg 2 beta 10+ and the master branch.

License
-------

BSD license.

