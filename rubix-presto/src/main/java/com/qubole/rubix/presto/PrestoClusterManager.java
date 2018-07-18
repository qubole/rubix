/**
 * Copyright (c) 2018. Qubole Inc
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
package com.qubole.rubix.presto;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by stagra on 14/1/16.
 */
public class PrestoClusterManager extends ClusterManager
{
  private boolean isMaster = true;
  private int serverPort = 8081;
  private String serverAddress = "localhost";
  static LoadingCache<String, List<String>> nodesCache;

  private Log log = LogFactory.getLog(PrestoClusterManager.class);

  // Safe to use single instance of HttpClient since Supplier.get() provides synchronization
  @Override
  public void initialize(final Configuration conf)
  {
    super.initialize(conf);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    nodesCache = CacheBuilder.newBuilder()
        .refreshAfterWrite(getNodeRefreshTime(), TimeUnit.SECONDS)
        .build(CacheLoader.asyncReloading(new CacheLoader<String, List<String>>() {
          @Override
          public List<String> load(String s)
              throws Exception
          {
            if (!isMaster) {
              // First time all nodes start assuming themselves as master and down the line figure out their role
              // Next time onwards, only master will be fetching the list of nodes
              return ImmutableList.of();
            }

            try {
              List<PrestoClusterManagerUtil.Stats> allNodes = PrestoClusterManagerUtil.getAllNodes(conf);
              if (allNodes == null) {
                isMaster = false;
                return ImmutableList.of();
              }

              if (allNodes.isEmpty()) {
                // Empty result set => server up and only master node running, return localhost has the only node
                // Do not need to consider failed nodes list as 1node cluster and server is up since it replied to allNodesRequest
                return ImmutableList.of(InetAddress.getLocalHost().getHostAddress());
              }

              List<PrestoClusterManagerUtil.Stats> failedNodes = PrestoClusterManagerUtil.getFailedNodes(conf);
              // keep only the healthy nodes
              allNodes.removeAll(failedNodes);

              Set<String> hosts = new HashSet<String>();

              for (PrestoClusterManagerUtil.Stats node : allNodes) {
                hosts.add(node.getUri().getHost());
              }

              if (hosts.isEmpty()) {
                // case of master only cluster
                hosts.add(InetAddress.getLocalHost().getHostAddress());
              }
              List<String> hostList = Lists.newArrayList(hosts.toArray(new String[0]));
              Collections.sort(hostList);
              return hostList;
            }
            catch (IOException e) {
              throw Throwables.propagate(e);
            }
          }
        }, executor));
  }

  @Override
  public boolean isMaster()
      throws ExecutionException
  {
    // issue get on nodesSupplier to ensure that isMaster is set correctly
    nodesCache.get("nodeList");
    return isMaster;
  }

  /*
   * This returns list of worker nodes when there are worker nodes in the cluster
   * If it is a single node cluster, it will return localhost information
   */
  @Override
  public List<String> getNodes()
  {
    try {
      return nodesCache.get("nodeList");
    }
    catch (ExecutionException e) {
      log.info("Error fetching node list : ", e);
    }
    return null;
  }

  @Override
  public Integer getNextRunningNodeIndex(int startIndex)
  {
    return startIndex;
  }

  @Override
  public Integer getPreviousRunningNodeIndex(int startIndex)
  {
    return startIndex;
  }

  @Override
  public ClusterType getClusterType()
  {
    return ClusterType.PRESTO_CLUSTER_MANAGER;
  }
}
