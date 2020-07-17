==============
Release 0.3.14
==============

Fixes and Features
------------------
* Fixed a regression from 0.3.11 which slows down split generation.
* Jmx stats refactoring to for better accounting of stats.
* Added support to plug in custom reporter for metrics that can send metrics to custom sinks. It can be set used by setting `rubix.metrics.reporters=CUSTOM` and providing implementation class using `rubix.metric-collector.impl`.