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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.net.UnknownHostException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * Created by stagra on 14/1/16.
 */

/*
 * This class should be implemented for each engine.
 * The implementation should return the nodes in a form which the scheduler of that engine can recognize and route the splits to
 */
public abstract class AsyncClusterManager extends ClusterManager
{
  private static Log log = LogFactory.getLog(AsyncClusterManager.class);

  private final AtomicReference<LoadingCache<String, Set<String>>> nodesCache = new AtomicReference<>();

  public void initialize(Configuration conf)
      throws UnknownHostException
  {
    super.initialize(conf);
    if (nodesCache.get() == null) {
      synchronized (nodesCache) {
        if (nodesCache.get() == null) {
          int nodeRefreshTime = CacheConfig.getClusterNodeRefreshTime(conf);
            ExecutorService executor =
                Executors.newSingleThreadExecutor(
                    r -> {
                      Thread t = Executors.defaultThreadFactory().newThread(r);
                      t.setName("rubix-get-nodes-thread");
                      t.setDaemon(true);
                      return t;
                    });

            nodesCache.set(
                CacheBuilder.newBuilder()
                    .refreshAfterWrite(nodeRefreshTime, TimeUnit.SECONDS)
                    .build(
                        CacheLoader.asyncReloading(
                            new CacheLoader<String, Set<String>>() {
                              @Override
                              public Set<String> load(String s) {
                                return getNodesAndUpdateState();
                              }
                            },
                            executor)));
        }
      }
    }
  }

  // Returns sorted list of nodes in the cluster
  public Set<String> getNodes()
  {
    requireNonNull(nodesCache, "ClusterManager used before initialization");
    return nodesCache.get().getUnchecked("nodes");
  }

}
