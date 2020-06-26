==============
Release 0.3.12
==============

Fixes and Features
------------------
* Prevent RemoteFetchProcessor from stopping on exception
* Fail fast in BookKeeper startup if no disks are available for caching
* Fix over-estimation of disk usage by cache
* Enable FileSystem object cache in Rubix servers
* Allow configuring Rubix via a separate xml file. ``rubix.site.location`` can be used to provide location of Rubix configuration file
* Removed shading of GCS connector to fix caching over GoogleHadoopFileSystem

New Extensions
--------------
* CachingPrestoAliyunOSSFileSystem: Caching over AliyunOSSFileSystem
* CachingPrestoAdlFileSystem: Caching over AdlFileSystem
