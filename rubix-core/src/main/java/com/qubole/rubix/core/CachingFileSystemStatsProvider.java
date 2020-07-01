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

import org.weakref.jmx.Managed;

import static com.qubole.rubix.spi.utils.DataSizeUnits.BYTES;

/**
 * Created by stagra on 28/1/16.
 */
public class CachingFileSystemStatsProvider
{
  private ReadRequestChainStats stats;

  public CachingFileSystemStatsProvider()
  {
    this.stats = new ReadRequestChainStats();
  }

  public synchronized void addReadRequestChainStats(ReadRequestChainStats newStats)
  {
    this.stats = this.stats.add(newStats);
  }

  public ReadRequestChainStats getStats()
  {
    return stats;
  }
}
