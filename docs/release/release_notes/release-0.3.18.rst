==============
Release 0.3.18
==============

Fixes and Features
------------------
* Cleanup cache directories before initializing the cache to correctly measure available disk space
* Update cache status with each successful readRequest in case of parallel warmup to minimize the errors in accounting disk space. Maximum read-request length is also limited to 100MB by default to minimize accounting errors.
