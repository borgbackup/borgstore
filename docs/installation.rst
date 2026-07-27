Installation
============

Minimal installation
--------------------

.. code-block:: bash

    pip install 'borgstore'

Only the `posixfs` (`file://...`) backend will be available.


Installation with optional dependencies
---------------------------------------

To also enable other backends or other optional features, use:

.. code-block:: bash

    pip install 'borgstore[rest,rclone,sftp,s3,blake3]'

For the available optional dependencies, see ``pyproject.toml``, section ``[project.optional-dependencies]``.

The ``blake3`` extra is not about a backend: it enables the "blake3" hash algorithm
for hash computations (blake3 is not part of ``hashlib``). For remote backends that
compute hashes server-side, it needs to be installed on the server.


Running the demo
----------------

Run this to get instructions on how to run the demo::

    python3 -m borgstore

