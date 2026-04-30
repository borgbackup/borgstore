REST Server
===========

BorgStore includes a simple REST server that can be used to provide remote access
to any BorgStore backend.

It can do some stuff server-side, which is usually not possible when using other
cloud storage servers:

- enforcing permissions
- server rejects store operation if content hashsum does not match expected
  hashsum (from http header X-Content-hash-sha256)
- server-side hash computation (e.g. sha256) for item content
- server-side defragmentation helper (copies blocks to new items)

Running the server
------------------

Run a server with a file: backend (for a local directory), using HTTP Basic Authentication::

    python3 -m borgstore.server.rest --host 127.0.0.1 --port 5618 \
            --username user --password pass \
            --backend file:///tmp/teststore

For production deployments, consider using systemd socket activation
(see contrib/server/nginx-systemd/README.md).

Accessing the server from a client
----------------------------------

The borgstore REST client can then access via::

    http://user:pass@127.0.0.1:5618/

Permissions
-----------

The REST server, when used with the ``posixfs`` backend, supports the same permissions
system as that backend (see above).

If ``--permissions`` is omitted, all operations are allowed.
To restrict permissions, pass a JSON-encoded permissions mapping via ``--permissions``.

Examples:

Read-only access::

    python3 -m borgstore.server.rest --host 127.0.0.1 --port 5618 \
            --username user --password pass \
            --backend file:///tmp/teststore \
            --permissions '{"": "lr"}'

No-delete, no-overwrite (allow adding new items)::

    python3 -m borgstore.server.rest --host 127.0.0.1 --port 5618 \
            --username user --password pass \
            --backend file:///tmp/teststore \
            --permissions '{"": "lrw"}'

Full access::

    python3 -m borgstore.server.rest --host 127.0.0.1 --port 5618 \
            --username user --password pass \
            --backend file:///tmp/teststore \
            --permissions '{"": "lrwWD"}'

BorgBackup shortcuts
~~~~~~~~~~~~~~~~~~~~

Instead of hand-crafting a JSON mapping, you can use a named shortcut tailored for
`BorgBackup <https://www.borgbackup.org/>`_ repositories:

``borgbackup-all``
    No permission restrictions — all operations are allowed (equivalent to omitting ``--permissions``).

``borgbackup-no-delete``
    Prevent deletion and overwriting of existing objects; new objects may still be added.

``borgbackup-write-only``
    Clients may store new data but cannot read existing data back (except for caches and metadata
    that borg needs internally).

``borgbackup-read-only``
    Clients may only list and read objects.

Example — restrict a backup server to no-delete access:

.. code-block:: bash

    python3 -m borgstore.server.rest --host 127.0.0.1 --port 5618 \
            --username user --password pass \
            --backend file:///home/user/repos/repo1 \
            --permissions borgbackup-no-delete

Custom JSON permissions
~~~~~~~~~~~~~~~~~~~~~~~

You can also pass an arbitrary JSON-encoded permissions mapping directly.

Hierarchical rules (list-only at root, read/write in ``data/``)::

    python3 -m borgstore.server.rest --host 127.0.0.1 --port 5618 \
            --username user --password pass \
            --backend file:///tmp/teststore \
            --permissions '{"": "l", "data": "lrw"}'

Quota
-----

The REST server, when used with the ``file:`` backend, optionally supports
quota tracking and enforcement.

Use the ``--quota`` argument to set a maximum storage size in bytes (default is
no quota tracking and enforcement).

When the quota is exceeded, ``store`` operations are rejected with HTTP 507
(Insufficient Storage).

Example — limit storage to 1 GiB:

.. code-block:: bash

    python3 -m borgstore.server.rest --host 127.0.0.1 --port 5618 \
            --username user --password pass \
            --backend file:///tmp/teststore \
            --quota 1073741824

