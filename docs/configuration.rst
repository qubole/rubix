.. _configuration:

=============
Configuration
=============

Cache
-----

+------------------------------------------+------------------------------------------------------------------------+-------------------+------------------+---------------+
| Option                                   | Description                                                            | Type              | Default          | Client/       |
|                                          |                                                                        |                   |                  | Server        |
+==========================================+========================================================================+===================+==================+===============+
| hadoop.cache.data.block.size             | The amount of data downloaded per block requested for caching.         | integer (bytes)   | 1048576 (1MB)    | C & S         |
|                                          | (if block size = 10MB, request for 45MB of data will download          |                   |                  |               |
|                                          | 5 blocks of 10MB)                                                      |                   |                  |               |
+------------------------------------------+------------------------------------------------------------------------+-------------------+------------------+---------------+
| hadoop.cache.data.dirprefix.list         | The list of directories to be used as parents for storing cache files. | list              | /media/ephemeral | C & S         |
|                                          | Example: **/media/ephemeral**\ 0/fcache/                               | (comma-separated) |                  |               |
+------------------------------------------+------------------------------------------------------------------------+-------------------+------------------+---------------+
| hadoop.cache.data.max.disks              | The number of (zero-indexed) disks within the parent directory to be   | integer           | 5                | C & S         |
|                                          | used for storing cached files.                                         |                   |                  |               |
|                                          | Example: /media/ephemeral\ **0** to /media/ephemeral\ **4**            |                   |                  |               |
+------------------------------------------+------------------------------------------------------------------------+-------------------+------------------+---------------+
| hadoop.cache.data.dirsuffix              | The name of the subdirectory to be used for storing cache files.       | string            | /fcache/         | C & S         |
|                                          | Example: /media/ephemeral0\ **/fcache/**                               |                   |                  |               |
+------------------------------------------+------------------------------------------------------------------------+-------------------+------------------+---------------+
| hadoop.cache.data.expiration.after-write | The time files will be kept in cache prior to eviction.                | integer (ms)      | MAX_VALUE        | S             |
+------------------------------------------+------------------------------------------------------------------------+-------------------+------------------+---------------+
| hadoop.cache.data.fullness.percentage    | The percentage of the disk space that will be filled with cached data  | integer (%)       | 80               | S             |
|                                          | before cached files will start being evicted.                          |                   |                  |               |
+------------------------------------------+------------------------------------------------------------------------+-------------------+------------------+---------------+
| hadoop.cache.data.strict.mode            | Propagate exceptions if there is an error while caching data if true;  | boolean           | false            | C             |
|                                          | otherwise fall back on reading data directly from remote file system   |                   |                  |               |
+------------------------------------------+------------------------------------------------------------------------+-------------------+------------------+---------------+
| rubix.enable.file.staleness-check        | When true, always check for updates to file metadata from remote       | boolean           | true             | S             |
|                                          | filesystem. When false, file metadata will be cached for a period of   |                   |                  |               |
|                                          | time before being fetched again.                                       |                   |                  |               |
+------------------------------------------+------------------------------------------------------------------------+-------------------+------------------+---------------+
| rubix.stale.fileinfo.expiry.period       | (**rubix.enable.file.staleness-check** must be false)                  | integer (s)       | 36000            | S             |
|                                          | The time file metadata will be cached before it will be fetched again  |                   |                  |               |
|                                          | from the remote filesystem.                                            |                   |                  |               |
+------------------------------------------+------------------------------------------------------------------------+-------------------+------------------+---------------+
| rubix.parallel.warmup                    | When true, cache will be warmed up asynchronously while not affecting  | boolean           | false            | C & S         |
|                                          | the current job.                                                       |                   |                  |               |
+------------------------------------------+------------------------------------------------------------------------+-------------------+------------------+---------------+

Network
-------

+------------------------------------------+------------------------------------------------------------------------------------+-------------------+------------------+---------------+
| Option                                   | Description                                                                        | Type              | Default          | Client/Server |
+==========================================+====================================================================================+===================+==================+===============+
| hadoop.cache.data.bookkeeper.port        | The port on which the BookKeeper server is listening.                              | integer           | 8899             | C & S         |
+------------------------------------------+------------------------------------------------------------------------------------+-------------------+------------------+---------------+
| hadoop.cache.data.local.server.port      | The port on which the Local Data Transfer server is listening.                     | integer           | 8898             | C             |
+------------------------------------------+------------------------------------------------------------------------------------+-------------------+------------------+---------------+
| hadoop.cache.data.client.num-retries     | The maximum number of retry attempts for executing calls to the BookKeeper server. | integer           | 3                | C & S         |
+------------------------------------------+------------------------------------------------------------------------------------+-------------------+------------------+---------------+
| hadoop.cache.data.client.timeout         | The maximum time to wait for a connection to the BookKeeper server.                | integer (ms)      | 10000            | C & S         |
+------------------------------------------+------------------------------------------------------------------------------------+-------------------+------------------+---------------+
| hadoop.cache.network.socket.read.timeout | The maximum time to wait when reading data from another node.                      | integer (ms)      | 30000            | C             |
+------------------------------------------+------------------------------------------------------------------------------------+-------------------+------------------+---------------+

Cluster
-------

+------------------------------------------+------------------------------------------------------------------------------------------------+-------------------+-------------------------------------------------+-----------------+
| Option                                   | Description                                                                                    | Type              | Default                                         | Client / Server |
+==========================================+================================================================================================+===================+=================================================+=================+
| rubix.hadoop.clustermanager.class        | The ``ClusterManager`` class to use for fetching node-related information for Hadoop clusters. | string            | com.qubole.rubix.hadoop2. Hadoop2ClusterManager | S               |
+------------------------------------------+------------------------------------------------------------------------------------------------+-------------------+-------------------------------------------------+-----------------+
| rubix.presto.clustermanager.class        | The ``ClusterManager`` class to use for fetching node-related information for Presto clusters. | string            | com.qubole.rubix.presto. PrestoClusterManager   | S               |
+------------------------------------------+------------------------------------------------------------------------------------------------+-------------------+-------------------------------------------------+-----------------+

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
