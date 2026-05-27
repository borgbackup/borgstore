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

- ``CachePolicy(mode=..., max_age=..., size=...)``
- ``{"mode": ..., "max_age": ..., "size": ...}``

``mode`` accepts ``CacheMode`` values or string aliases:

- ``CacheMode.C_OFF`` or ``"off"``: bypass cache completely.
- ``CacheMode.C_MIRROR`` or ``"mirror"``: always read from primary backend,
  but update the cache after successful primary backend reads and writes.
- ``CacheMode.C_WRITETHROUGH`` or ``"writethrough"``: read-through +
  write-through.
  For now, only content-hash addressed namespaces should use this mode.

``max_age`` is optional and expressed in seconds since last access. The default
is ``None`` (no age limit).

``size`` is optional and expressed in bytes. It sets a per-namespace cache
size budget enforced by evicting least-recently-used items until the namespace
total size is within the configured budget.

Example::

    from borgstore.store import Store, CacheMode

    store = Store(
        url="sftp://user@host/repo",
        levels={"data": [2], "meta": [1]},
        cache={
            "data": {
                "mode": "writethrough",
                "max_age": 3600,
                "size": 4 * 1024**3,
            },
            "meta": {"mode": CacheMode.C_MIRROR},
        },
        cache_url="file:///home/user/.cache/borgstore/repo",
    )

Behavior
--------

- Cache keys are identical to primary backend keys (same nesting).
- Soft-deleted items are cached under the same ``.del`` name as primary.
- Soft delete/undelete renames cache entries as well.
- On ``Store.open()`` and ``Store.close()``, cache-enabled namespaces are scanned
  to clean up the cache. Cleanup order per namespace is:

  1. remove expired cache objects when ``max_age`` is configured,
  2. if ``size`` is configured, evict the least-recently-used remaining items
     until the namespace total size is ``<= size``.

  Expired entries are always removed first, even if total size is already below
  the ``size`` limit.
- Cache failures are non-fatal and logged as warnings.

Manual Cache Invalidation
-------------------------

If you need to programmatically clear or invalidate parts of the cache (for
example, to resolve stale objects after primary backend deletes by other
clients, or if cache corruption is suspected), you can use the
``cache_invalidate`` method:

- To invalidate a single item::

      store.cache_invalidate("data/00000000")

- To invalidate all cached items in a specific namespace (e.g. ``"data/"``)::

      store.cache_invalidate("data/")

- To invalidate all cached items across all configured namespaces, pass
  ``ROOTNS``::

      from borgstore.constants import ROOTNS
      store.cache_invalidate(ROOTNS)

Limitations
-----------

- Eviction by ``max_age`` or ``size`` is open-time and close-time only
  (``Store.open()`` / ``Store.close()``), not continuous during
  ``store()``/``load()`` operations.
- No proactive cache validation/revalidation.
- If an object is deleted in the primary backend by another client, the local
  cache will still have a stale object.
- ``max_age`` and LRU-by-``size`` depend on backend ``ItemInfo.atime`` support.
  If ``atime`` is 0 (not implemented):

  - using ``max_age`` would empty the cache on ``Store.open()`` or ``Store.close()``
  - using ``size`` would not work in LRU order, because order can't be
    determined
- If a partial range ``load`` call for an object in a cached namespace causes
  a cache miss, the full object will be read from the primary backend and the
  cache will be populated with the full object.

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
