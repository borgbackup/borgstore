Store
=====

Overview
--------

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
- hash: computes the hexdigest for the content of an item (given its key).
- delete: immediately remove an item from the store (given its key).
- move: implements renaming, soft delete/undelete, and moving to the current
  nesting level.
- defrag: general purpose defragmentation helper (copies blocks to new items)
- quota: return quota limit and usage (-1 if quotas not enabled or not supported)
- stats: API call counters, time spent in API methods, data volume/throughput.
- latency/bandwidth emulator: can emulate higher latency (via BORGSTORE_LATENCY
  [us]) and lower bandwidth (via BORGSTORE_BANDWIDTH [bit/s]) than what is
  actually provided by the backend.

Store operations (and per-op timing and volume) are logged at DEBUG log level.

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

Automatic Nesting
-----------------

For the Store user, items have names such as:

- namespace/0123456789abcdef...
- namespace/abcdef0123456789...

If there are very many items in the namespace, this could lead to scalability
issues in the backend. The Store implementation therefore offers transparent
nesting, so that internally the backend API is called with names such as:

- namespace/01/23/45/0123456789abcdef...
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

