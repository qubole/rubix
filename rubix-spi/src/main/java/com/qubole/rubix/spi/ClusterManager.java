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
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.hadoop.conf.Configuration;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by stagra on 14/1/16.
 */

/*
 * This class should be implemented for each engine.
 * The implementation should return the nodes in a form which the scheduler of that engine can recognize and route the splits to
 */
public abstract class ClusterManager
{
  private final AtomicReference<LoadingCache<String, List<String>>> nodesCache = new AtomicReference<>();

  public abstract ClusterType getClusterType();

  /*
   * gets the nodes as per the engine
   * returns null in case node list cannot be fetched
   * returns empty in case of master-only setup
   */
  protected abstract List<String> getNodesInternal();

  public void initialize(Configuration conf)
  {
    if (nodesCache.get() == null) {
      synchronized (nodesCache) {
        if (nodesCache.get() == null) {
          int nodeRefreshTime = CacheConfig.getClusterNodeRefreshTime(conf);
          ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setName("rubix-get-nodes-thread");
            t.setDaemon(true);
            return t;
          });

          nodesCache.set(
                  CacheBuilder.newBuilder()
                  .refreshAfterWrite(nodeRefreshTime, TimeUnit.SECONDS)
                  .build(CacheLoader.asyncReloading(new CacheLoader<String, List<String>>()
                  {
                    @Override
                    public List<String> load(String s)
                    {
                      List<String> allNodes = getNodesInternal();
                      if (allNodes == null) {
                        return ImmutableList.of();
                      }

                      if (allNodes.isEmpty()) {
                        // Empty result set => server up and only master node running, return localhost has the only node
                        // Do not need to consider failed nodes list as 1node cluster and server is up since it replied to allNodesRequest
                        return ImmutableList.of(InetAddress.getLocalHost().getHostAddress());
                      }

                      Collections.sort(allNodes);
                      return allNodes;
                    }
                  }, executor)));
        }
      }
    }
  }

  public int getNodeIndex(int numNodes, String key)
  {
    HashFunction hf = Hashing.md5();
    HashCode hc = hf.hashString(key, Charsets.UTF_8);
    return Hashing.consistentHash(hc, numNodes);
  }

  // Returns sorted list of nodes in the cluster
  public List<String> getNodes()
  {
    return nodesCache.get().getUnchecked("nodes");
  }
}
