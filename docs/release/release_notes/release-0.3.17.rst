==============
Release 0.3.17
==============

.. Note:: This release removes shading of thrift jars from rubix-spi. If you are using rubix-spi as a dependency, you will not find the thrift classes from this jar and you will need to use rubix-build jars instead.

Fixes and Features
------------------
* Improvements in consistent hashing logic to minimize redistributions during change in the membership of the cluster
* Moved shading of Thrift classes into a common sub-module rubix-build. Project is now traversable in IDE and mvn test works at the root. Clients should now include rubix-build artifact instead of including sub-modules independently
* Consider requests served from another node's cache under cache hit
* Added `total_system_source_mb_read` stat in detailed metrics to show total data read from source: during read + warmups

