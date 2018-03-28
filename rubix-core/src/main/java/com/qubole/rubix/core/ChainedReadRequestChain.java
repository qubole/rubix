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

import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by qubole on 14/9/16.
 */
public class ChainedReadRequestChain extends ReadRequestChain
{
  private List<ReadRequestChain> readRequestChains = new ArrayList<>();

  public ChainedReadRequestChain addReadRequestChain(ReadRequestChain readRequestChain)
  {
    readRequestChains.add(readRequestChain);
    return this;
  }

  @Override
  public Integer call()
      throws Exception
  {
    int read = 0;
    for (ReadRequestChain readRequestChain : readRequestChains) {
      readRequestChain.lock();
      read += readRequestChain.call();
    }
    return read;
  }

  @Override
  public ReadRequestChainStats getStats()
  {
    ReadRequestChainStats stats = new ReadRequestChainStats();
    for (ReadRequestChain readRequestChain : readRequestChains) {
      stats = stats.add(readRequestChain.getStats());
    }
    return stats;
  }

  @Override
  public void cancel()
  {
    for (ReadRequestChain readRequestChain : readRequestChains) {
      readRequestChain.cancel();
    }
  }

  public void updateCacheStatus(String remotePath, long fileSize, long lastModified, int blockSize, Configuration conf)
  {
    for (ReadRequestChain readRequestChain : readRequestChains) {
      readRequestChain.updateCacheStatus(remotePath, fileSize, lastModified, blockSize, conf);
    }
  }
}
