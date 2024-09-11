BorgStore
=========

A key/value store implementation in Python, supporting multiple backends,
data redundancy and distribution.

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
- list: flat list of the items in the given namespace, with or without soft
  deleted items.
- store: write a new item into the store (giving its key/value pair)
- load: read a value from the store (giving its key), partial loads giving
  offset and/or size are supported.
- info: get information about an item via its key (exists? size? ...)
- delete: immediately remove an item from the store (giving its key)
- move: implements rename, soft delete / undelete, move to current
  nesting level
- stats: api call counters, time spent in api methods, data volume/throughput
- latency/bandwidth emulator: can emulate higher latency and lower bandwidth
  than what is actually provided by the backend.

Automatic Nesting
-----------------

For the Store user, items have names like e.g.:

namespace/0123456789abcdef...
namespace/abcdef0123456789...

If there are very many items in the namespace, this could lead to scalability
issues in the backend, thus the Store implementation offers transparent
nesting, so that internally the Backend API will be called with
names like e.g.:

namespace/01/23/56/0123456789abcdef...
namespace/ab/cd/ef/abcdef0123456789...

The nesting depth can be configured from 0 (= no nesting) to N levels and
there can be different nesting configurations depending on the namespace.

The Store supports operating at different nesting levels in the same
namespace at the same time.

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

Currently, these storage backends are implemented:

- POSIX filesystems (namespaces: directories, values: in key-named files)
- SFTP (access a server via sftp, namespaces: directories, values: in key-named files)
- (more might come in future)

MStore
------

API of MStore is very similar to Store, but instead of directly using one backend
only (like Store does), it uses multiple Stores internally to implement:

- redundancy (keep same data at multiple places)
- distribution (keep different data at multiple places)

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

Want a demo?
------------

Run this to get instructions how to run the demo:

python3 -m borgstore

State of this project
---------------------

**API is still unstable and expected to change as development goes on.**

**There will be no data migration tools involving development/testing releases,
like e.g. upgrading a store from alpha1 to alpha2 or beta13 to release.**

There are tests and they succeed for the basic functionality, so some of the
stuff is already working well.

There might be missing features or optimization potential, feedback welcome!

There are a lot of possible, but still missing backends (like e.g. for cloud
storage). If you want to create and support one: pull requests are welcome.

Borg?
-----

Please note that this code is currently **not** used by the stable release of
BorgBackup (aka "borg"), but only by borg2 beta 10+ and master branch.

License
-------

BSD license.

