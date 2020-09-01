=============================
Release 0.3.17 (next release)
=============================

Fixes and Features
------------------
* Added `total_system_source_mb_read` stat in detailed metrics to show total data read from source: during read + warmups
* Moved shading of Thrift classes into a common sub-module rubix-build. Project is now traversable in IDE and mvn test works at the root. Clients should now include rubix-build artifact instead of including sub-modules independently