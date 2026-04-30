BorgStore
=========

borgstore implements a general purpose key/value store in Python.

Overview
--------

Keys are simple strings like `config/main` or `data/0123456789abcdef` `[str]`
(config and data are namespaces here). Values are binary objects `[bytes]`.

The `Store` class is the high-level API, so you can comfortably work with the
kv store without caring for low-level details.

The `backends` package has misc. storage backend implementations.

The `server` package has a REST server implementation, complementing the REST
client functionality in the `rest` backend. To actually store stuff, the REST server
can use any backend internally, e.g. the `posixfs` backend.

Store features
--------------

- supports URLs, like `file:///srv/borgstore` or `https://myserver/path`
- easy to use, high-level `Store` API: create/destroy, open/close, list,
  load/store, delete, move, soft delete/undelete, hash, defrag, ...
- name nesting / unnesting, recursive directory listing
- statistics collection
- latency/bandwidth emulator

Backend features
----------------

- existing backends for local filesystem, sftp, REST, S3 / B2 (native) and
  many other cloud storage protocols via rclone
- new backends are simple to implement
- key validation
- partial loads / range requests
- stored object hashing
- stored object defragmentation
- quota support (only `posixfs`)
- permissions checking (only `posixfs`)

REST server features
--------------------

- server-side permissions/quota enforcement
- server-side hashsum check of transferred objects before storing
- network traffic optimization by doing stuff server-side:

  - stored object hashing
  - stored object defragmentation
- the REST server can internally use any backend for storage, e.g. `posixfs`
- for the REST server, we provide CI tested configs for:

  - an nginx-based reverse proxy
  - systemd-based on-demand `borgstore.server` process creation

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
