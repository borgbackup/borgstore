Store caching
=============

The ``Store`` can optionally use a second backend as a local cache for selected
namespaces, which is especially useful when the primary backend is remote
slower or otherwise more "expensive" than the cache.

Configuration
-------------

- ``cache_url`` or ``cache_backend``: where cached data is stored
- ``cache``: mapping of namespace to cache mode

Cache modes are configured with ``CacheMode`` or string aliases:

- ``CacheMode.C_OFF`` or ``"off"``: bypass cache completely.
- ``CacheMode.C_MIRROR`` or ``"mirror"``: always read from primary backend,
  but update the cache after successful primary backend reads and writes.
- ``CacheMode.C_CACHE`` or ``"cache"``: read-through + write-through.
  For now, only content-hash addressed namespaces should use this mode.

Behavior
--------

- Cache keys are identical to primary backend keys (same nesting).
- Soft-deleted items are cached under the same ``.del`` name as primary.
- Soft delete/undelete (``move(delete=True|undelete=True)``) renames cache
  entries in lockstep with primary backend names.
- Cache failures are non-fatal and logged as warnings.

Limitations
-----------

- No cache eviction.
- No proactive cache validation/revalidation.
- If an object is deleted in the primary backend by another client, the local
  cache will still have a stale object.

Statistics
----------

``Store.stats`` includes cache counters:

- ``cache_hits``
- ``cache_misses``
- ``cache_errors``
- ``cache_bytes_read``
- ``cache_bytes_written``
- ``cache_hit_ratio``
