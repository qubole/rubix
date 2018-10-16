.. _metrics:

=======
Metrics
=======

Health
------

Metrics relating to daemon & service health.

+--------------------------------------------------+--------------------------------------------+-----------------------------------------+
| Metric                                           | Description                                | Abnormalities                           |
+==================================================+============================================+=========================================+
| rubix.bookkeeper.gauge.live_workers              | The number of workers currently reporting  | Mismatch with number reported by engine |
|                                                  | to the master node.                        | (Presto, Hive, etc.)                    |
+--------------------------------------------------+--------------------------------------------+-----------------------------------------+
| rubix.bookkeeper.gauge.caching_validated_workers | The number of workers reporting caching    | Mismatch with live worker count         |
|                                                  | validation success.                        | (one or more workers failed validation) |
+--------------------------------------------------+--------------------------------------------+-----------------------------------------+
| rubix.bookkeeper.gauge.file_validated_workers    | The number of workers reporting file       | Mismatch with live worker count         |
|                                                  | validation success.                        | (one or more workers failed validation) |
+--------------------------------------------------+--------------------------------------------+-----------------------------------------+

Cache
-----

Metrics relating to cache interactions.

+------------------------------------------------+--------------------------------------------+--------------------------------+
| Metric                                         | Description                                | Abnormalities                  |
+================================================+============================================+================================+
| rubix.bookkeeper.count.cache_eviction          | The number of entries removed from the     | High number of cache evictions |
|                                                | local cache due to size constraints.       |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.count.cache_invalidation      | The number of entries evicted from the     |                                |
|                                                | local cache explicitly.                    |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.count.cache_expiry            | The number of entries evicted from the     |                                |
|                                                | local cache once expired.                  |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.gauge.cache_hit_rate          | The percentage of cache hits for the       | Cache hit rate near 0%         |
|                                                | local cache.                               |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.gauge.cache_miss_rate         | The percentage of cache misses for the     | Cache miss rate near 100%      |
|                                                | local cache.                               |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.gauge.cache_size_mb           | The current size of the local cache in MB. |                                |
|                                                |                                            |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.count.total_request           | The total number of requests made for data |                                |
|                                                | cached locally.                            |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.count.cache_request           | The number of requests made for data       | No cache requests made         |
|                                                | cached locally.                            |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.count.nonlocal_request        | The number of requests made for data       | No non-local requests made     |
|                                                | cached on another node.                    |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.count.remote_request          | The number of requests made for data not   | No remote requests made        |
|                                                | currently cached.                          |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.count.total_async_request     | The total number of requests made for      |                                |
|                                                | asynchronously fetching data.              |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.count.processed_async_request | The total number of asynchronous requests  |                                |
|                                                | that have already been processed.          |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.gauge.async_queue_size        | The current number of queued               | High queue size                |
|                                                | asynchronous requests.                     | (requests not being processed) |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.count.async_downloaded_mb     | The amount of data asynchronously          |                                |
|                                                | downloaded, in MB                          |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+

JVM
---

Metrics relating to JVM statistics, supplied by the Dropwizard Metrics ``metrics-jvm`` module. (https://metrics.dropwizard.io/3.1.0/manual/jvm/)

+--------------------------------+----------------------------------------+---------------+
| Metric                         | Description                            | Abnormalities |
+================================+========================================+===============+
| rubix.bookkeeper.jvm.gc.*      | Metrics relating to garbage collection |               |
| rubix.ldts.jvm.gc.*            | (GarbageCollectorMetricSet)            |               |
+--------------------------------+----------------------------------------+---------------+
| rubix.bookkeeper.jvm.memory.*  | Metrics relating to memory usage       |               |
| rubix.ldts.jvm.memory.*        | (MemoryUsageGaugeSet)                  |               |
+--------------------------------+----------------------------------------+---------------+
| rubix.bookkeeper.jvm.threads.* | Metrics relating to thread states      |               |
| rubix.ldts.jvm.threads.*       | (CachedThreadStatesGaugeSet)           |               |
+--------------------------------+----------------------------------------+---------------+

Validation
----------

Metrics relating to validation.

+---------------------------------------------------+------------------------------------------+----------------------------+
| Metric                                            | Description                              | Abnormalities              |
+===================================================+==========================================+============================+
| rubix.bookkeeper.gauge.caching_validation_success | Indicates the success/failure of caching | Caching validation failure |
|                                                   | validation for the node.                 | (reporting 0)              |
+---------------------------------------------------+------------------------------------------+----------------------------+
| rubix.bookkeeper.gauge.file_validation_success    | Indicates the success/failure of file    | File validation failure    |
|                                                   | validation for the node.                 | (reporting 0)              |
+---------------------------------------------------+------------------------------------------+----------------------------+
