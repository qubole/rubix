.. _configuration:

=============
Configuration
=============

Cache
-----

+------------------------------------------+------------------------------------------------------------------------+-------------------+------------------+---------------+---------------+
| Option                                   | Description                                                            | Type              | Default          | Client/       | Applicable to |
|                                          |                                                                        |                   |                  | Server        | Embedded mode |
+==========================================+========================================================================+===================+==================+===============+===============+
| rubix.cache.block.size                   | The amount of data downloaded per block requested for caching.         | integer (bytes)   | 1048576 (1MB)    | C & S         |               |
|                                          | (if block size = 10MB, request for 45MB of data will download          |                   |                  |               | Yes           |
|                                          | 5 blocks of 10MB)                                                      |                   |                  |               |               |
+------------------------------------------+------------------------------------------------------------------------+-------------------+------------------+---------------+---------------+
| rubix.cache.dirprefix.list               | The list of directories to be used as parents for storing cache files. | list              | /media/ephemeral | C & S         | No            |
|                                          | Example: **/media/ephemeral**\ 0/fcache/                               | (comma-separated) |                  |               |               |
+------------------------------------------+------------------------------------------------------------------------+-------------------+------------------+---------------+---------------+
| rubix.cache.max.disks                    | The number of (zero-indexed) disks within the parent directory to be   | integer           | 5                | C & S         |               |
|                                          | used for storing cached files.                                         |                   |                  |               | No            |
|                                          | Example: /media/ephemeral\ **0** to /media/ephemeral\ **4**            |                   |                  |               |               |
+------------------------------------------+------------------------------------------------------------------------+-------------------+------------------+---------------+---------------+
| rubix.cache.dirsuffix                    | The name of the subdirectory to be used for storing cache files.       | string            | /fcache/         | C & S         | Yes           |
|                                          | Example: /media/ephemeral0\ **/fcache/**                               |                   |                  |               |               |
+------------------------------------------+------------------------------------------------------------------------+-------------------+------------------+---------------+---------------+
| rubix.cache.expiration.after-write       | The time files will be kept in cache prior to eviction.                | integer (ms)      | MAX_VALUE        | S             | Yes           |
+------------------------------------------+------------------------------------------------------------------------+-------------------+------------------+---------------+---------------+
| rubix.cache.usage.percentage             | The percentage of the disk space that will be filled with cached data  | integer (%)       | 80               | S             | Yes           |
|                                          | before cached files will start being evicted.                          |                   |                  |               |               |
+------------------------------------------+------------------------------------------------------------------------+-------------------+------------------+---------------+---------------+
| rubix.cache.strict.mode                  | Propagate exceptions if there is an error while caching data if true;  | boolean           | false            | C             | No            |
|                                          | otherwise fall back on reading data directly from remote file system.  |                   |                  |               |               |
+------------------------------------------+------------------------------------------------------------------------+-------------------+------------------+---------------+---------------+
| rubix.cache.file.staleness-check.enable  | When true, always check for updates to file metadata from remote       | boolean           | true             | S             |               |
|                                          | filesystem. When false, file metadata will be cached for a period of   |                   |                  |               | Yes           |
|                                          | time before being fetched again.                                       |                   |                  |               |               |
+------------------------------------------+------------------------------------------------------------------------+-------------------+------------------+---------------+---------------+
| rubix.cache.stale.fileinfo.expiry.period | (**rubix.cache.file.staleness-check.enable** must be false)            | integer (s)       | 36000            | S             |               |
|                                          | The time file metadata will be cached before it will be fetched again  |                   |                  |               | Yes           |
|                                          | from the remote filesystem.                                            |                   |                  |               |               |
+------------------------------------------+------------------------------------------------------------------------+-------------------+------------------+---------------+---------------+
| rubix.cache.parallel.warmup              | When true, cache will be warmed up asynchronously.                     | boolean           | false            | C & S         | Yes           |
+------------------------------------------+------------------------------------------------------------------------+-------------------+------------------+---------------+---------------+
| rubix.cache.dummy.mode                   | When true, the cache is not populated with data and queries read data  | boolean           | false            | C             |               |
|                                          | directly from the source, but metadata is updated so that statistics   |                   |                  |               | Yes           |
|                                          | such as hitrate, cache reads etc. can be collected as if the data was  |                   |                  |               |               |
|                                          | cached.                                                                |                   |                  |               |               |
+------------------------------------------+------------------------------------------------------------------------+-------------------+------------------+---------------+---------------+


Network
-------

+------------------------------------------+------------------------------------------------------------------------------------+-------------------+------------------+---------------+---------------+
| Option                                   | Description                                                                        | Type              | Default          | Client/Server | Applicable to |
|                                          |                                                                                    |                   |                  |               | Embedded mode |
+==========================================+====================================================================================+===================+==================+===============+===============+
| rubix.network.bookkeeper.server.port     | The port on which the BookKeeper server is listening.                              | integer           | 8899             | C & S         |     No        |
+------------------------------------------+------------------------------------------------------------------------------------+-------------------+------------------+---------------+---------------+
| rubix.network.local.transfer.server.port | The port on which the Local Data Transfer server is listening.                     | integer           | 8898             | C             |     No        |
+------------------------------------------+------------------------------------------------------------------------------------+-------------------+------------------+---------------+---------------+
| rubix.network.client.num-retries         | The maximum number of retry attempts for executing calls to the BookKeeper server. | integer           | 3                | C & S         |     Yes       |
+------------------------------------------+------------------------------------------------------------------------------------+-------------------+------------------+---------------+---------------+
| rubix.network.server.connect.timeout     | The maximum time to wait for a connection to the BookKeeper server.                | integer (ms)      | 1000             | C & S         |     Yes       |
+------------------------------------------+------------------------------------------------------------------------------------+-------------------+------------------+---------------+---------------+
| rubix.network.server.socket.timeout      | The maximum time to wait for a response to requests sent to the BookKeeper server. | integer (ms)      | 3000             | C & S         |     Yes       |
+------------------------------------------+------------------------------------------------------------------------------------+-------------------+------------------+---------------+---------------+
| rubix.network.client.read.timeout        | The maximum time to wait when reading data from another node.                      | integer (ms)      | 3000             | C             |     Yes       |
+------------------------------------------+------------------------------------------------------------------------------------+-------------------+------------------+---------------+---------------+


Cluster
-------

+------------------------------------------+------------------------------------------------------------------------------------------------+-------------------+-------------------------------------------------+-----------------+---------------+
| Option                                   | Description                                                                                    | Type              | Default                                         | Client / Server | Applicable to |
|                                          |                                                                                                |                   |                                                 |                 | Embedded mode |
+==========================================+================================================================================================+===================+=================================================+=================+===============+
| rubix.cluster.node.refresh.time          | The frequency at which the cluster node membership will be checked.                            | integer (s)       | 300 sec                                         | C & S           | Yes           |
+------------------------------------------+------------------------------------------------------------------------------------------------+-------------------+-------------------------------------------------+-----------------+---------------+
| rubix.cluster.manager.hadoop.class       | The ``ClusterManager`` class to use for fetching node-related information for Hadoop clusters. | string            | com.qubole.rubix.hadoop2. Hadoop2ClusterManager | C & S           | No            |
+------------------------------------------+------------------------------------------------------------------------------------------------+-------------------+-------------------------------------------------+-----------------+---------------+
| rubix.cluster.manager.presto.class       | The ``ClusterManager`` class to use for fetching node-related information for Presto clusters. | string            | com.qubole.rubix.presto. PrestoClusterManager   | C & S           | No            |
+------------------------------------------+------------------------------------------------------------------------------------------------+-------------------+-------------------------------------------------+-----------------+---------------+
| rubix.cluster.type                       | The type of cluster where RubiX is running.                                                    | integer           | 3 (For Test Clusters)                           | S               |               |
|                                          | For Hadoop/Spark cluster the value is 0                                                        |                   |                                                 |                 | No            |
|                                          | For Presto cluster the value is 1                                                              |                   |                                                 |                 |               |
+------------------------------------------+------------------------------------------------------------------------------------------------+-------------------+-------------------------------------------------+-----------------+---------------+

Metrics
-------

+------------------------------------------+--------------------------------------------------------------------------------+-------------------+------------------------------------------------+-----------------+
| Option                                   | Description                                                                    | Type              | Default                                        | Client / Server |
+==========================================+================================================================================+===================+================================================+=================+
| rubix.metrics.cache.enabled              | Collect cache-level metrics if true.                                           | boolean           | true                                           | S               |
+------------------------------------------+--------------------------------------------------------------------------------+-------------------+------------------------------------------------+-----------------+
| rubix.metrics.health.enabled             | Collect heartbeat metrics if true.                                             | boolean           | true                                           | S               |
+------------------------------------------+--------------------------------------------------------------------------------+-------------------+------------------------------------------------+-----------------+
| rubix.metrics.jvm.enabled                | Collect JVM-level metrics if true.                                             | boolean           | false                                          | S               |
+------------------------------------------+--------------------------------------------------------------------------------+-------------------+------------------------------------------------+-----------------+
| rubix.metrics.reporters                  | The reporters to be used for collecting metrics.                               | list              | JMX,GANGLIA                                    | S               |
|                                          | Options: JMX, GANGLIA                                                          | (comma-separated) |                                                |                 |
+------------------------------------------+--------------------------------------------------------------------------------+-------------------+------------------------------------------------+-----------------+
| rubix.metrics.reporting.interval         | The interval at which all registered reporters will report their metrics.      | integer (ms)      | 10000                                          | S               |
+------------------------------------------+--------------------------------------------------------------------------------+-------------------+------------------------------------------------+-----------------+
| rubix.metrics.ganglia.host               | The host at which the Ganglia server (gmond) is running.                       | string            | 127.0.0.1 (localhost)                          | S               |
+------------------------------------------+--------------------------------------------------------------------------------+-------------------+------------------------------------------------+-----------------+
| rubix.metrics.ganglia.port               | The port on which the Ganglia server (gmond) is listening.                     | integer           | 8649                                           | S               |
+------------------------------------------+--------------------------------------------------------------------------------+-------------------+------------------------------------------------+-----------------+
