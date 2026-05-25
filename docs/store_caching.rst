Store caching
=============

The ``Store`` can optionally use a second backend as a local cache for selected
namespaces, which is especially useful when the primary backend is remote
slower or otherwise more "expensive" than the cache.

Configuration
-------------

- ``cache_url`` or ``cache_backend``: where cached data is stored
- ``cache``: mapping of namespace to cache policy

Each cache policy can be provided either as:

- ``CachePolicy(mode=..., max_age=...)``
- ``{"mode": ..., "max_age": ...}``

``mode`` accepts ``CacheMode`` values or string aliases:

- ``CacheMode.C_OFF`` or ``"off"``: bypass cache completely.
- ``CacheMode.C_MIRROR`` or ``"mirror"``: always read from primary backend,
  but update the cache after successful primary backend reads and writes.
- ``CacheMode.C_CACHE`` or ``"cache"``: read-through + write-through.
  For now, only content-hash addressed namespaces should use this mode.

``max_age`` is optional and expressed in seconds since last access. The default
is ``None`` (no age limit).

Example::

    from borgstore.store import Store, CacheMode

    store = Store(
        url="sftp://user@host/repo",
        levels={"data": [2], "meta": [1]},
        cache={
            "data": {"mode": "cache", "max_age": 3600},
            "meta": {"mode": CacheMode.C_MIRROR},
        },
        cache_url="file:///home/user/.cache/borgstore/repo",
    )

Behavior
--------

- Cache keys are identical to primary backend keys (same nesting).
- Soft-deleted items are cached under the same ``.del`` name as primary.
- Soft delete/undelete (``move(delete=True|undelete=True)``) renames cache
  entries in lockstep with primary backend names.
- If ``max_age`` is configured and a cache item is expired, it is deleted from
  the cache and treated as a cache miss.
- On ``Store.close()``, cache-enabled namespaces with ``max_age`` configured are
  scanned and expired cache objects are removed before closing the cache
  backend.
- Cache failures are non-fatal and logged as warnings.

Limitations
-----------

- No cache eviction.
- No proactive cache validation/revalidation.
- If an object is deleted in the primary backend by another client, the local
  cache will still have a stale object.
- ``max_age`` depends on backend ``ItemInfo.atime`` support. If ``atime`` is 0
  (not implemented), age-based caching behaves as immediate expiry.

Statistics
----------

``Store.stats`` includes cache counters:

- ``cache_hits``
- ``cache_misses``
- ``cache_errors``
- ``cache_bytes_read``
- ``cache_bytes_written``
- ``cache_hit_ratio``
- ``cache_disabled``
