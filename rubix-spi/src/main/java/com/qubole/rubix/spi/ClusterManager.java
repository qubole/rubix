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
package com.qubole.rubix.spi;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by stagra on 14/1/16.
 */

/*
 * This class should be implemented for each engine.
 * The implementation should return the nodes in a form which the scheduler of that engine can recognize and route the splits to
 */
public abstract class ClusterManager
{
  private long splitSize;
  private int nodeRefreshTime;

  public abstract ClusterType getClusterType();

  public void initialize(Configuration conf)

  {
    splitSize = CacheConfig.getCacheFileSplitSize(conf);
    nodeRefreshTime = CacheConfig.getClusterNodeRefreshTime(conf);
  }

  public int getNodeIndex(int numNodes, String key)
  {
    HashFunction hf = Hashing.md5();
    HashCode hc = hf.hashString(key, Charsets.UTF_8);
    int initialNodeIndex = Hashing.consistentHash(hc, numNodes);
    int finalNodeIndex = initialNodeIndex;
    if (hc.asInt() % 2 == 0) {
      finalNodeIndex = getNextRunningNodeIndex(initialNodeIndex);
    }
    else {
      finalNodeIndex = getPreviousRunningNodeIndex(initialNodeIndex);
    }

    return finalNodeIndex;
  }

  // This is the size in which the file will be logically divided into splits
  public long getSplitSize()
  {
    return splitSize;
  }

  public int getNodeRefreshTime()
  {
    return nodeRefreshTime;
  }

  public abstract boolean isMaster()
      throws ExecutionException;

  // Nodes format as per the note above
  // Should return sorted list
  public abstract List<String> getNodes();

  public abstract Integer getNextRunningNodeIndex(int startIndex);

  public abstract Integer getPreviousRunningNodeIndex(int startIndex);
}
