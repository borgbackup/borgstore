ChangeLog
=========

Version 0.0.3 2024-09-17
------------------------

- sftp: add support for ~/.ssh/config, #37
- sftp: username is optional, #27
- load known_hosts, remove AutoAddPolicy, #39
- store: raise BE specific exceptions, #34
- add Store.stats property, #25
- bandwidth emulation via BORGSTORE_BANDWIDTH [bit/s], #24
- latency emulation via BORGSTORE_LATENCY [us], #24
- fix demo code, also output stats
- tests: BORGSTORE_TEST_SFTP_URL to set sftp test server


Version 0.0.2 2024-09-10
------------------------

- sftp backend: use paramiko's client.posix_rename, #17
- posixfs backend: hack: accept file://relative/path, #23
- support / test on Python 3.13, #21


Version 0.0.1 2024-08-23
------------------------

First PyPi release.
