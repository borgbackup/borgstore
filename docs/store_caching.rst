Store caching
=============

The ``Store`` can optionally use a second backend as a local cache for selected
namespaces, which is especially useful when the primary backend is remote,
slower or otherwise more "expensive" than the cache.

Configuration
-------------

- ``cache_url`` or ``cache_backend``: where cached data is stored
- ``config``: mapping of namespace to its configuration dict, containing
  nesting levels and cache policy settings.

Each namespace configuration dictionary can have:

- ``levels``: a required list of integers specifying nesting levels.
- ``cache``: optional cache mode, accepting ``CacheMode`` values or string
  aliases:

  - ``CacheMode.C_OFF`` or ``"off"``: bypass cache completely (default).
  - ``CacheMode.C_MIRROR`` or ``"mirror"``: always read from primary backend,
    but update the cache after successful primary backend reads and writes.
  - ``CacheMode.C_WRITETHROUGH`` or ``"writethrough"``: read-through +
    write-through. For now, only content-hash addressed namespaces should
    use this mode.

- ``max_age``: optional maximum age expressed in seconds since last access.
  The default is ``None`` (no age limit).
- ``size``: optional maximum size in bytes. It sets a per-namespace cache
  size budget enforced by evicting least-recently-used items until the
  namespace total size is within the configured budget. Items bigger than
  ``size`` are not cached.

Example::

    from borgstore.store import Store, CacheMode

    store = Store(
        url="sftp://user@host/repo",
        config={
            "data": {
                "levels": [2],
                "cache": "writethrough",
                "max_age": 3600,
                "size": 4 * 1024**3,
            },
            "meta": {
                "levels": [1],
                "cache": CacheMode.C_MIRROR,
            },
        },
        cache_url="file:///home/user/.cache/borgstore/repo",
    )

Behavior
--------

- Cache keys are identical to primary backend keys (same nesting).
- Soft-deleted items are cached under the same ``.del`` name as primary.
- Soft delete/undelete renames cache entries as well.
- Cache failures are non-fatal and logged as warnings.

Eviction
--------

For each namespace that has a ``max_age`` or ``size`` limit, the ``Store`` keeps
an in-memory index of the cached items (name, size, time of last use) while it
is opened. The index is ordered by the last use *by this store*: a cache hit,
putting an item into the cache or moving it counts as using it.

- On ``Store.open()`` and ``Store.close()``, the namespace is scanned (listed) and
  cleaned up. When scanning, items the store did not know yet are ordered by
  their ``ItemInfo.atime``.
- Before an item is put into the cache (by ``store()`` or by a ``load()`` that
  was a cache miss), room is made for it, so the namespace total size stays
  ``<= size`` also while the store is in use.

Cleanup order per namespace is:

1. remove expired cache objects when ``max_age`` is configured,
2. if ``size`` is configured, evict the least-recently-used remaining items
   until the namespace total size (plus the size of the item to put into the
   cache) is ``<= size``.

Expired entries are always removed first, even if total size is already below
the ``size`` limit. A cache hit never expires anything: an expired item that is
still in the cache is served (and that counts as using it).

Shared caches
-------------

Multiple clients (``Store`` instances, also in different processes) may use the
same cache at the same time, e.g. a cache directory shared by several
processes working with the same content-hash addressed data:

- The posixfs backend stores items atomically, so a client never sees or
  evicts an incompletely written cache item.
- If another client has evicted an item, that is a cache miss and the item
  gets cached again.
- A client only knows what it has put into the cache itself and what it has
  seen when it last scanned the namespace. Thus, a namespace with a ``size``
  limit is scanned again after the client has put more than ``size / 4`` bytes
  into it. With N clients, the namespace total size can temporarily reach about
  ``size * (1 + N / 4)``.
- Clients do not see each other's cache hits (see the ``atime`` limitation
  below), so a client might evict an item another client frequently uses.
- If the clients use different limits for the same namespace, the smallest
  limits win.

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

- No proactive cache validation/revalidation.
- If an object is deleted in the primary backend by another client, the local
  cache will still have a stale object.
- For items a ``Store`` has not used itself since it was opened (items cached
  in a previous session or by another client), ``max_age`` and LRU-by-``size``
  depend on backend ``ItemInfo.atime`` support, currently that is supported by
  ``posixfs`` and ``REST`` backends. Filesystems often do not update the atime
  for each read (e.g. ``relatime`` or ``noatime`` mounts), so it can be older
  than the real last use.
  If ``atime`` is 0 (not implemented):

  - using ``max_age`` would remove these items from the cache when it is
    scanned
  - using ``size`` would not evict these items in LRU order, because their
    order can't be determined
- If a partial range ``load`` call for an object in a cached namespace causes
  a cache miss, the full object will be read from the primary backend and the
  cache will be populated with the full object (if it is not bigger than
  ``size``).

Statistics
----------

``Store.stats`` includes cache counters:

- ``backend_load_volume``
- ``backend_store_volume``
- ``backend_load_calls``
- ``backend_store_calls``
- ``backend_delete_calls``
- ``cache_disabled``
- ``cache_hits``
- ``cache_misses``
- ``cache_hit_ratio``
- ``cache_errors``
- ``cache_load_volume``
- ``cache_store_volume``
- ``cache_load_calls``
- ``cache_store_calls``
- ``cache_delete_calls``
