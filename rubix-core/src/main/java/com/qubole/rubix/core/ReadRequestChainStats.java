/**
 * Copyright (c) 2019. Qubole Inc
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.qubole.rubix.core;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.RetryingPooledBookkeeperClient;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by stagra on 28/1/16.
 */
public class ReadRequestChainStats
{
  public static final String DOWNLOADED_FOR_NON_LOCAL_METRIC = "downloaded_for_non_local";
  public static final String EXTRA_READ_FOR_NON_LOCAL_METRIC = "extra_read_for_non_local";
  public static final String WARMUP_TIME_NON_LOCAL_METRIC = "warmup_penalty_non_local";
  public static final String DOWNLOADED_FOR_PARALLEL_WARMUP_METRIC = "downloaded_for_parallel_warmup";
  public static final String PARALLEL_DOWNLOAD_TIME_METRIC = "parallel_download_time";

  private static final long UNKNOWN_VALUE = -1;

  // bookKeeperStats are fetched over thrift in standalone mode
  // and are usually needed for multiple getters in this class
  // cache the server response to minimize thrift calls but evict aggressively
  private static final Cache<String, Map<String, Long>> bookKeeperStats = CacheBuilder
          .newBuilder()
          .expireAfterWrite(5, TimeUnit.SECONDS)
          .build();
  private static final String STATS_KEY = "stats";

  // RemoteRRC
  private long remoteRRCDataRead;
  private long remoteRRCExtraDataRead;
  private long remoteRRCWarmupTime;
  private long remoteRRCRequests;

  // CachedRRC
  private long cachedRRCDataRead;
  private long cachedRRCRequests;

  // DirectRRC
  private long directRRCDataRead;
  private long directRRCRequests;

  // NonLocalRRC
  private long nonLocalRRCDataRead;
  private long nonLocalRRCRequests;

  private int corruptedFileCount;

  public ReadRequestChainStats add(ReadRequestChainStats other)
  {
    remoteRRCDataRead += other.getRemoteRRCDataRead();
    remoteRRCExtraDataRead += other.getRemoteRRCExtraDataRead();
    remoteRRCWarmupTime += other.getRemoteRRCWarmupTime();
    remoteRRCRequests += other.getRemoteRRCRequests();
    cachedRRCDataRead += other.getCachedRRCDataRead();
    cachedRRCRequests += other.getCachedRRCRequests();
    directRRCDataRead += other.getDirectRRCDataRead();
    directRRCRequests += other.getDirectRRCRequests();
    nonLocalRRCDataRead += other.getNonLocalRRCDataRead();
    nonLocalRRCRequests += other.getNonLocalRRCRequests();
    corruptedFileCount += other.getCorruptedFileCount();
    return this;
  }

  public long getRemoteRRCDataRead()
  {
    return remoteRRCDataRead;
  }

  public ReadRequestChainStats setRemoteRRCDataRead(long remoteRRCDataRead)
  {
    this.remoteRRCDataRead = remoteRRCDataRead;
    return this;
  }

  public long getRemoteRRCExtraDataRead()
  {
    return remoteRRCExtraDataRead;
  }

  public ReadRequestChainStats setRemoteRRCExtraDataRead(long remoteRRCExtraDataRead)
  {
    this.remoteRRCExtraDataRead = remoteRRCExtraDataRead;
    return this;
  }

  public long getRemoteRRCWarmupTime()
  {
    return remoteRRCWarmupTime;
  }

  public ReadRequestChainStats setRemoteRRCWarmupTime(long remoteRRCWarmupTime)
  {
    this.remoteRRCWarmupTime = remoteRRCWarmupTime;
    return this;
  }

  public long getRemoteRRCRequests()
  {
    return remoteRRCRequests;
  }

  public ReadRequestChainStats setRemoteRRCRequests(long remoteRRCRequests)
  {
    this.remoteRRCRequests = remoteRRCRequests;
    return this;
  }

  public long getCachedRRCDataRead()
  {
    return cachedRRCDataRead;
  }

  public ReadRequestChainStats setCachedRRCDataRead(long cachedRRCDataRead)
  {
    this.cachedRRCDataRead = cachedRRCDataRead;
    return this;
  }

  public long getCachedRRCRequests()
  {
    return cachedRRCRequests;
  }

  public ReadRequestChainStats setCachedRRCRequests(long cachedRRCRequests)
  {
    this.cachedRRCRequests = cachedRRCRequests;
    return this;
  }

  public long getDirectRRCDataRead()
  {
    return directRRCDataRead;
  }

  public ReadRequestChainStats setDirectRRCDataRead(long directRRCDataRead)
  {
    this.directRRCDataRead = directRRCDataRead;
    return this;
  }

  public long getDirectRRCRequests()
  {
    return directRRCRequests;
  }

  public ReadRequestChainStats setDirectRRCRequests(long directRRCRequests)
  {
    this.directRRCRequests = directRRCRequests;
    return this;
  }

  public long getNonLocalRRCDataRead()
  {
    return nonLocalRRCDataRead;
  }

  public ReadRequestChainStats setNonLocalRRCDataRead(long nonLocalRRCDataRead)
  {
    this.nonLocalRRCDataRead = nonLocalRRCDataRead;
    return this;
  }

  public long getNonLocalRRCRequests()
  {
    return nonLocalRRCRequests;
  }

  public ReadRequestChainStats setNonLocalRRCRequests(long nonLocalRRCRequests)
  {
    this.nonLocalRRCRequests = nonLocalRRCRequests;
    return this;
  }

  public int getCorruptedFileCount()
  {
    return this.corruptedFileCount;
  }

  public ReadRequestChainStats setCorruptedFileCount(int corruptedFileCount)
  {
    this.corruptedFileCount = corruptedFileCount;
    return this;
  }

  public long getDownloadedFromSourceForNonLocalRequests(BookKeeperFactory bookKeeperFactory, Configuration conf)
  {
    return getBookKeeperStats(bookKeeperFactory, conf).getOrDefault(DOWNLOADED_FOR_NON_LOCAL_METRIC, UNKNOWN_VALUE);
  }

  public long getExtraReadFromSourceForNonLocalRequests(BookKeeperFactory bookKeeperFactory, Configuration conf)
  {
    return getBookKeeperStats(bookKeeperFactory, conf).getOrDefault(EXTRA_READ_FOR_NON_LOCAL_METRIC, UNKNOWN_VALUE);
  }

  public long getWarmupTimeForNonLocalRequests(BookKeeperFactory bookKeeperFactory, Configuration conf)
  {
    return getBookKeeperStats(bookKeeperFactory, conf).getOrDefault(WARMUP_TIME_NON_LOCAL_METRIC, UNKNOWN_VALUE);
  }

  public long getDownloadedFromSourceParallel(BookKeeperFactory bookKeeperFactory, Configuration conf)
  {
    return getBookKeeperStats(bookKeeperFactory, conf).getOrDefault(DOWNLOADED_FOR_PARALLEL_WARMUP_METRIC, UNKNOWN_VALUE);
  }

  public long getTotalTimeForParallelDownload(BookKeeperFactory bookKeeperFactory, Configuration conf)
  {
    return getBookKeeperStats(bookKeeperFactory, conf).getOrDefault(PARALLEL_DOWNLOAD_TIME_METRIC, UNKNOWN_VALUE);
  }

  private Map<String, Long> getBookKeeperStats(BookKeeperFactory bookKeeperFactory, Configuration conf)
  {
    try {
      return bookKeeperStats.get(STATS_KEY,() ->
      {
        try(RetryingPooledBookkeeperClient client = bookKeeperFactory.createBookKeeperClient(conf)) {
          return client.getReadRequestChainStats();
        }
      });
    }
    catch(Exception e) {
      return ImmutableMap.of();
    }
  }
}
