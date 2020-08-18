.. _metrics:

=======
Metrics
=======

BookKeeper Server Metrics
-------------------------

These metrics are available on the BookKeeper server.

**Health Metrics**

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

**Cache Metrics**

Metrics relating to cache interactions.

+------------------------------------------------+--------------------------------------------+--------------------------------+
| Metric                                         | Description                                | Abnormalities                  |
+================================================+============================================+================================+
| rubix.bookkeeper.gauge.cache_size_mb           | The current size of the local cache in MB. | Cache size is bigger than      |
|                                                |                                            | configured capacity            |
+------------------------------------------------+--------------------------------------------+--------------------------------+
| rubix.bookkeeper.gauge.available_cache_size_mb | The current disk space available for       |                                |
|                                                | cache in MB.                               |                                |
|                                                |                                            |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+
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
| rubix.bookkeeper.count.async_download_time     | Total time spent on downloading data in sec|                                |
|                                                |                                            |                                |
+------------------------------------------------+--------------------------------------------+--------------------------------+

**JVM Metrics**

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

Client side Metrics
-------------------

These metrics are available on the client side i.e. Presto or Spark where the jobs to read data are run.

Client side Metrics is divided into two:

1. Basic stats: These stats are available under name `rubix:name=stats`
2. Detailed stats: These stats are available under name `rubix:name=stats,type=detailed`

If Rubix is used in embedded mode, an engine specific suffix is added to these names, e.g., Presto adds `catalog=<catalog_name>` suffix.

Following sections cover the metrics available under both these types in detail.

**Basic stats**

+------------------------------------------------+--------------------------------------------+
| Metric                                         | Description                                |
+================================================+============================================+
| mb_read_from_cache                             | Data read from cache                       |
+------------------------------------------------+--------------------------------------------+
| mb_read_from_source                            | Data read from Source                      |
+------------------------------------------------+--------------------------------------------+
| cache_hit                                      | Cache Hit ratio, between 0 and 1           |
+------------------------------------------------+--------------------------------------------+

**Detailed Stats**

+------------------------------------------------+--------------------------------------------+
| Metric                                         | Description                                |
+================================================+============================================+
| mb_read_from_cache                             | Data read from cache                       |
+------------------------------------------------+--------------------------------------------+
| mb_read_from_source                            | Data read from Source                      |
+------------------------------------------------+--------------------------------------------+
| cache_hit                                      | Cache Hit ratio, between 0 and 1           |
+------------------------------------------------+--------------------------------------------+
| cached_rrc_data_read                           | Data read from local cache                 |
+------------------------------------------------+--------------------------------------------+
| cached_rrc_requests                            | Number of requests served from local cache |
+------------------------------------------------+--------------------------------------------+
| direct_rrc_data_read                           | Data read from Source, but not cached      |
+------------------------------------------------+--------------------------------------------+
| direct_rrc_requests                            | Number of requests served from source      |
|                                                | but not cached                             |
+------------------------------------------------+--------------------------------------------+
| nonlocal_rrc_requests                          | Number of request served from              |
|                                                | non-local caches                           |
+------------------------------------------------+--------------------------------------------+
| nonlocal_rrc_data_read                         | Data read from non-local caches            |
+------------------------------------------------+--------------------------------------------+
| remote_rrc_data_read                           | Data downloaded for local requests         |
+------------------------------------------------+--------------------------------------------+
| remote_rrc_extra_data_read                     | Extra data downloaded for local requests   |
|                                                | due to block boundary alignments           |
+------------------------------------------------+--------------------------------------------+
| remote_rrc_requests                            | Number of local requests on cold cache     |
+------------------------------------------------+--------------------------------------------+
| remote_rrc_warmup_penalty                      | Seconds spent in copying data into cache   |
|                                                | while downloading data for local requests  |
+------------------------------------------------+--------------------------------------------+
| corrupted_file_count                           | Number of corrupted files that have been   |
|                                                | invalidated                                |
+------------------------------------------------+--------------------------------------------+
| bks_data_downloaded_in_parallel_warmup         | Data downloaded by BookKeeper, when        |
|                                                | parallel warmup is enabled                 |
+------------------------------------------------+--------------------------------------------+
| bks_time_for_parallel_downloads                | Seconds spent in downloading data          |
|                                                | when parallel warmup is enabled            |
+------------------------------------------------+--------------------------------------------+
| bks_data_downloaded_in_read_through            | Data downloaded by BookKeeper              |
|                                                | when parallel warmup is disabled           |
+------------------------------------------------+--------------------------------------------+
| bks_extra_data_read_in_read_through            | Extra  data read from source due to block  |
|                                                | alignment by BookKeeper                    |
|                                                | when parallel warmup is disabled           |
+------------------------------------------------+--------------------------------------------+
| bks_warmup_penalty_in_read_through             | Seconds spent in copying data into cache   |
|                                                | by BookKeeper                              |
|                                                | when parallel warmup is disabled           |
+------------------------------------------------+--------------------------------------------+

*Data unit in all metrics above is MB*