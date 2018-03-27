/**
 * Copyright (c) 2016. Qubole Inc
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

import org.weakref.jmx.Managed;

/**
 * Created by stagra on 28/1/16.
 */
public class CachingFileSystemStats
{
  private ReadRequestChainStats stats;

  private int bytesInMb = 1024 * 1024;

  public CachingFileSystemStats()
  {
    this.stats = new ReadRequestChainStats();
  }

  public synchronized void addReadRequestChainStats(ReadRequestChainStats newStats)
  {
    this.stats = this.stats.add(newStats);
  }

  @Managed(description = "Gets the total size in MB that was downloaded from Remote files")
  public double getReadFromRemote()
  {
    return (double) stats.getTotalDownloaded() / bytesInMb;
  }

  @Managed(description = "Gets the total size in MB that was read for non local splits")
  public double getNonLocalDataRead()
  {
    return (double) stats.getNonLocalDataRead() / bytesInMb;
  }

  @Managed(description = "Gets the total number of non local reads that were done")
  public long getNonLocalReads()
  {
    return stats.getNonLocalReads();
  }

  @Managed(description = "Gets the extra  MB read from remote files due to block alignment")
  public double getExtraReadFromRemote()
  {
    return (double) stats.getExtraRead() / bytesInMb;
  }

  @Managed(description = "Gets the MB read from cache")
  public double getReadFromCache()
  {
    return (double) stats.getCachedDataRead() / bytesInMb;
  }

  @Managed(description = "Gets the total number of request going out to Remote")
  public long getRemoteReads()
  {
    return stats.getRemoteReads();
  }

  @Managed(description = "Gets the total number of request served from cache")
  public long getCachedReads()
  {
    return stats.getCachedReads();
  }

  @Managed(description = "Gets the time in seconds spent in copying data into cache buffers. This is aggregated across all threads hence not the apparent penalty")
  public long getWarmupPenalty()
  {
    return stats.getWarmupPenalty() / 1000000000;
  }

  @Managed
  public double getHitRate()
  {
    return (double) stats.getCachedReads() / (stats.getCachedReads() + stats.getRemoteReads());
  }

  @Managed
  public double getMissRate()
  {
    return (double) stats.getRemoteReads() / (stats.getCachedReads() + stats.getRemoteReads());
  }
}
