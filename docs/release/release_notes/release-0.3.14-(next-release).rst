=============================
Release 0.3.14 (next release)
=============================

Fixes and Features
------------------
* Fixed a regression from 0.3.11 which slows down split generation.
* Jmx stats refactoring to for better accounting of stats.
* Add CustomMetricsReporter interface. It can be set used by setting `rubix.metrics.reporters=CUSTOM` and providing implementation class using `rubix.metric-collector.impl`.