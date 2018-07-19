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

import com.facebook.presto.hive.$internal.com.google.common.collect.ImmutableMap;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Abhishek on 7/11/18.
 */
public class PrestoClusterManagerV2 extends ClusterManager
{
  private boolean isMaster = true;
  private int serverPort = 8081;
  private String serverAddress = "localhost";
  static LoadingCache<String, Map<String, String>> nodesCache;

  // This is the master list that will keep track of the current nodes and the lost nodes
  static Map<String, String> allNodesMap;

  private Log log = LogFactory.getLog(PrestoClusterManagerV2.class);

  // Safe to use single instance of HttpClient since Supplier.get() provides synchronization
  @Override
  public void initialize(final Configuration conf)
  {
    super.initialize(conf);
    allNodesMap = new HashMap<String, String>();

    ExecutorService executor = Executors.newSingleThreadExecutor();
    nodesCache = CacheBuilder.newBuilder()
        .refreshAfterWrite(getNodeRefreshTime(), TimeUnit.SECONDS)
        .build(CacheLoader.asyncReloading(new CacheLoader<String, Map<String, String>>() {
          @Override
          public Map<String, String> load(String s)
              throws Exception
          {
            if (!isMaster) {
              // First time all nodes start assuming themselves as master and down the line figure out their role
              // Next time onwards, only master will be fetching the list of nodes
              return ImmutableMap.of();
            }

            try {
              List<PrestoClusterManagerUtil.Stats> allNodes = PrestoClusterManagerUtil.getAllNodes(conf);
              if (allNodes == null) {
                isMaster = false;
                // if allNodes is null, it means we got error from the getAllNodes. Return the master list
                return allNodesMap;
              }

              if (allNodes.isEmpty()) {
                if (allNodesMap.isEmpty()) {
                  return ImmutableMap.of(InetAddress.getLocalHost().getHostAddress(), "ACTIVATED");
                }
                else {
                  return allNodesMap;
                }
              }

              for (PrestoClusterManagerUtil.Stats node : allNodes) {
                if (!allNodesMap.containsKey(node.getUri().getHost())) {
                  allNodesMap.put(node.getUri().getHost(), "ACTIVATED");
                }
              }

              List<PrestoClusterManagerUtil.Stats> failedNodes = PrestoClusterManagerUtil.getFailedNodes(conf);

              for (PrestoClusterManagerUtil.Stats node : failedNodes) {
                if (!allNodesMap.containsKey(node.getUri().getHost())) {
                  log.warn("Failed Node " + node.getUri().getHost() + " not present in the master list");
                }
                allNodesMap.put(node.getUri().getHost(), "DEACTIVATED");
              }

              return allNodesMap;
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
      return new ArrayList<>(nodesCache.get("nodeList").keySet());
    }
    catch (ExecutionException e) {
      log.info("Error fetching node list : ", e);
    }
    return null;
  }

  @Override
  public Integer getNextRunningNodeIndex(int startIndex)
  {
    try {
      List<String> nodeList = new ArrayList<>(nodesCache.get("nodeList").keySet());
      for (int i = startIndex; i < (startIndex + nodeList.size()); i++) {
        int index = i >= nodeList.size() ? (i - nodeList.size()) : i;
        String nodeState = nodesCache.get("nodeList").get(nodeList.get(index));
        if (nodeState.equalsIgnoreCase("ACTIVATED")) {
          return index;
        }
      }
    }
    catch (ExecutionException e) {
      e.printStackTrace();
    }

    return null;
  }

  @Override
  public Integer getPreviousRunningNodeIndex(int startIndex)
  {
    try {
      List<String> nodeList = new ArrayList<>(nodesCache.get("nodeList").keySet());
      for (int i = startIndex; i >= 0; i--) {
        String nodeState = nodesCache.get("nodeList").get(nodeList.get(i));
        if (nodeState.equalsIgnoreCase("ACTIVATED")) {
          return i;
        }
      }

      for (int i = nodeList.size() - 1; i > startIndex; i--) {
        String nodeState = nodesCache.get("nodeList").get(nodeList.get(i));
        if (nodeState.equalsIgnoreCase("ACTIVATED")) {
          return i;
        }
      }
    }
    catch (ExecutionException e) {
      e.printStackTrace();
    }

    return null;
  }

  @Override
  public ClusterType getClusterType()
  {
    return ClusterType.PRESTO_CLUSTER_MANAGER;
  }
}
