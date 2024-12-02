BorgStore
=========

A key/value store implementation in Python, supporting multiple backends.

Keys
----

A key (str) can look like:

- 0123456789abcdef...  (usually a long, hex-encoded hash value)
- Any other pure ASCII string without "/" or ".." or " ".


Namespaces
----------

To keep stuff apart, keys should get prefixed with a namespace, like:

- config/settings
- meta/0123456789abcdef...
- data/0123456789abcdef...

Please note:

1. you should always use namespaces.
2. nested namespaces like namespace1/namespace2/key are not supported.
3. the code could work without a namespace (namespace ""), but then you
   can't add another namespace later, because then you would have created
   nested namespaces.

Values
------

Values can be any arbitrary binary data (bytes).

Store Operations
----------------

The high-level Store API implementation transparently deals with nesting and
soft deletion, so the caller doesn't have to care much for that and the Backend
API can be much simpler:

- create/destroy: initialize or remove the whole store.
- list: flat list of the items in the given namespace (by default only not
  soft deleted items, optionally only soft deleted items).
- store: write a new item into the store (giving its key/value pair)
- load: read a value from the store (giving its key), partial loads giving
  offset and/or size are supported.
- info: get information about an item via its key (exists? size? ...)
- delete: immediately remove an item from the store (giving its key)
- move: implements rename, soft delete / undelete, move to current
  nesting level
- stats: api call counters, time spent in api methods, data volume/throughput
- latency/bandwidth emulator: can emulate higher latency (via BORGSTORE_LATENCY
  [us]) and lower bandwidth (via BORGSTORE_BANDWIDTH [bit/s]) than what is
  actually provided by the backend.

Automatic Nesting
-----------------

For the Store user, items have names like e.g.:

- namespace/0123456789abcdef...
- namespace/abcdef0123456789...

If there are very many items in the namespace, this could lead to scalability
issues in the backend, thus the Store implementation offers transparent
nesting, so that internally the Backend API will be called with
names like e.g.:

- namespace/01/23/56/0123456789abcdef...
- namespace/ab/cd/ef/abcdef0123456789...

The nesting depth can be configured from 0 (= no nesting) to N levels and
there can be different nesting configurations depending on the namespace.

The Store supports operating at different nesting levels in the same
namespace at the same time.

When using nesting depth > 0, the backends will assume that keys are hashes
(have hex digits) because some backends will want to pre-create the nesting
directories at backend initialization time to optimize for better performance
while using the backend.

Soft deletion
-------------

To soft delete an item (so its value could be still read or it could be
undeleted), the store just renames the item, appending ".del" to its name.

Undelete reverses this by removing the ".del" suffix from the name.

Some store operations have a boolean flag "deleted" to choose whether they
shall consider soft deleted items.

Backends
--------

The backend API is rather simple, one only needs to provide some very
basic operations.

Existing backends are listed below, more might come in future.

posixfs
~~~~~~~

Use storage on a local POSIX filesystem:

- URL: ``file:///absolute/path``
- it is the caller's task to create an absolute fs path from a relative one.
- namespaces: directories
- values: in key-named files
- pre-creates nesting directories

sftp
~~~~

Use storage on a sftp server:

- URL: ``sftp://user@server:port/relative/path`` (strongly recommended)

  For user's and admin's convenience, mapping the URL path to the server fs path
  depends on the server configuration (home directory, sshd/sftpd config, ...).
  Usually the path is relative to the user's home directory.
- URL: ``sftp://user@server:port//absolute/path``

  As this uses an absolute path, things are more difficult here:

  - user's config might break if server admin moves a user home to a new location.
  - users must know the full absolute path of space they have permission to use.
- namespaces: directories
- values: in key-named files
- pre-creates nesting directories

rclone
~~~~~~

Use storage on any of the many cloud providers `rclone <https://rclone.org/>`_ supports:

- URL: ``rclone:remote:path``, we just prefix "rclone:" and give all to the right
  of that to rclone, see: https://rclone.org/docs/#syntax-of-remote-paths
- implementation of this primarily depends on the specific remote.


Scalability
-----------

- Count of key/value pairs stored in a namespace: automatic nesting is
  provided for keys to address common scalability issues.
- Key size: there are no special provisions for extremely long keys (like:
  more than backend limitations). Usually this is not a problem though.
- Value size: there are no special provisions for dealing with large value
  sizes (like: more than free memory, more than backend storage limitations,
  etc.). If one deals with very large values, one usually cuts them into
  chunks before storing them into the store.
- Partial loads improve performance by avoiding a full load if only a part
  of the value is needed (e.g. a header with metadata).

Installation
------------

Install without the ``sftp:`` backend::

    pip install borgstore

Install with the ``sftp:`` backend (more dependencies)::

   pip install "borgstore[sftp]"

Please note that ``rclone:`` also supports sftp remotes.

Want a demo?
------------

Run this to get instructions how to run the demo:

python3 -m borgstore

State of this project
---------------------

**API is still unstable and expected to change as development goes on.**

**As long as the API is unstable, there will be no data migration tools,
like e.g. for upgrading an existing store's data to a new release.**

There are tests and they succeed for the basic functionality, so some of the
stuff is already working well.

There might be missing features or optimization potential, feedback welcome!

There are a lot of possible, but still missing backends. If you want to create
and support one: pull requests are welcome.

Borg?
-----

Please note that this code is currently **not** used by the stable release of
BorgBackup (aka "borg"), but only by borg2 beta 10+ and master branch.

License
-------

BSD license.

