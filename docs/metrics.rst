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
|                                                  | to the master node.                        | (Presto, Spark, etc.)                   |
+--------------------------------------------------+--------------------------------------------+-----------------------------------------+
| rubix.bookkeeper.gauge.caching_validated_workers | The number of workers reporting caching    | Mismatch with live worker count         |
|                                                  | validation success.                        | (one or more workers failed validation) |
+--------------------------------------------------+--------------------------------------------+-----------------------------------------+

Cache
-----

Metrics relating to cache interactions.

+------------------------------------------------+--------------------------------------------+--------------------------------+
| Metric                                         | Description                                | Abnormalities                  |
+================================================+============================================+================================+
| rubix.bookkeeper.count.cache_eviction          | The number of files removed from the       | No cache evictions & cache has |
|                                                | local cache due to size constraints.       | exceeded configured capacity   |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.count.cache_invalidation      | The number of files invalidated from the   |                                |
|                                                | local cache when the source file has been  |                                |
|                                                | modified.                                  |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.count.cache_expiry            | The number of files removed from the       |                                |
|                                                | local cache once expired.                  |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.gauge.cache_hit_rate          | The percentage of cache hits for the       | Cache hit rate near 0%         |
|                                                | local cache.                               |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.gauge.cache_miss_rate         | The percentage of cache misses for the     | Cache miss rate near 100%      |
|                                                | local cache.                               |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.gauge.cache_size_mb           | The current size of the local cache in MB. | Cache size is bigger than      |
|                                                |                                            | configured capacity            |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.count.total_request           | The total number of requests made          |                                |
|                                                | to read data.                              |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.count.cache_request           | The number of requests made to read data   | No cache requests made         |
|                                                | cached locally.                            |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.count.nonlocal_request        | The number of requests made to read data   | No non-local requests made     |
|                                                | from another node.                         |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.count.remote_request          | The number of requests made to download    | No remote requests made        |
|                                                | data from the data store.                  |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.count.total_async_request     | The total number of requests made to       |                                |
|                                                | download data asynchronously.              |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.count.processed_async_request | The total number of asynchronous download  |                                |
|                                                | requests that have already been processed. |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.gauge.async_queue_size        | The current number of queued               | High queue size                |
|                                                | asynchronous download requests.            | (requests not being processed) |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.count.async_downloaded_mb     | The amount of data asynchronously          |                                |
|                                                | downloaded, in MB.                         |                                |
|                                                | (If there are no cache evictions, this     |                                |
|                                                | should match ``cache_size_mb``.)           |                                |
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
