.. _metrics:

=======
Metrics
=======

These are the metrics currently available for RubiX.

+-----------------------------------------+--------------------------------------------+
| RubiX Metric                            | Description                                |
+=========================================+============================================+
| rubix.bookkeeper.live_workers.gauge     | The number of workers currently reporting  |
|                                         | to the master node.                        |
+-----------------------------------------+--------------------------------------------+
| rubix.bookkeeper.cache_eviction.count   | The number of entries evicted from the     |
|                                         | local cache.                               |
+-----------------------------------------+--------------------------------------------+
| rubix.bookkeeper.cache_hit_rate.gauge   | The percentage of cache hits for the       |
|                                         | local cache.                               |
+-----------------------------------------+--------------------------------------------+
| rubix.bookkeeper.cache_miss_rate.gauge  | The percentage of cache misses for the     |
|                                         | local cache.                               |
+-----------------------------------------+--------------------------------------------+
| rubix.bookkeeper.cache_size.gauge       | The current size of the local cache in MB. |
|                                         |                                            |
+-----------------------------------------+--------------------------------------------+
| rubix.bookkeeper.local_request.count    | The number of requests made for data       |
|                                         | cached locally.                            |
+-----------------------------------------+--------------------------------------------+
| rubix.bookkeeper.nonlocal_request.count | The number of requests made for data       |
|                                         | cached on another node.                    |
+-----------------------------------------+--------------------------------------------+
| rubix.bookkeeper.remote_request.count   | The number of requests made for data not   |
|                                         | currently cached.                          |
+-----------------------------------------+--------------------------------------------+