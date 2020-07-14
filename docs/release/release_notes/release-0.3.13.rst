==============
Release 0.3.13
==============

Fixes and Features
------------------
* Generation numbers are added for files cached on disk to avoid several race conditions with invalidations
* Scavenger service has been added to reap the idle connections
* Local Data Server connections are now pooled
* Fail fast when BookKeeper or Local Data Server sockets cannot be created
* Use bounded thread pools BookKeeper and Local Data Server
* Parallel warmup is now enabled by default
