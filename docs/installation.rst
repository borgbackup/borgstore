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

    pip install 'borgstore[rest,rclone,sftp,s3]'

For the available optional dependencies, see ``pyproject.toml``, section ``[project.optional-dependencies]``.


Running the demo
----------------

Run this to get instructions on how to run the demo::

    python3 -m borgstore

