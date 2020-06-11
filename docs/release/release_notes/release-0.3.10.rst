==============
Release 0.3.10
==============

* Bypass getFileInfo network call when staleness check is enabled
* Remove runtime dependencies to fix ClassNotFound errors in Embedded mode
* Ensure InputStream for DirectReadRequestChain is always closed
* Make CachingFileSystem extend FilterFileSystem
* Fix connection leak in RetryingPooledThriftClient that happens if a connection terminates with an exception

