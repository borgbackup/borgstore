ChangeLog
=========

Version 0.0.3 2024-09-xx
------------------------

- store: raise BE specific exceptions, #34
- add Store.stats property, #25
- bandwidth emulation via BORGSTORE_BANDWIDTH [bit/s], #24
- latency emulation via BORGSTORE_LATENCY [us], #24
- sftp: username is optional
- fix demo code, also output stats


Version 0.0.2 2024-09-10
------------------------

- sftp backend: use paramiko's client.posix_rename, #17
- posixfs backend: hack: accept file://relative/path, #23
- support / test on Python 3.13, #21


Version 0.0.1 2024-08-23
------------------------

First PyPi release.
