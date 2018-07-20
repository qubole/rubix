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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    allNodesMap = new LinkedHashMap<String, String>();

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

            return loadNodesCache(conf);
          }
        }, executor));
  }

  @VisibleForTesting
  Map<String, String> loadNodesCache(Configuration conf) throws Exception
  {
    try {
      List<PrestoClusterManagerUtil.Stats> nodeListFromServer = PrestoClusterManagerUtil.getAllNodes(conf);

      if (nodeListFromServer == null) {
        isMaster = false;
        // if nodeListFromServer is null, it means we got error from the getAllNodes. Return the master list
        return allNodesMap;
      }

      // If getAllNodes returned an empty list, we need to check whether the master is empty or not
      if (nodeListFromServer.isEmpty()) {
        if (allNodesMap.isEmpty()) {
          // if the master list is empty, return localhost as activated state
          return ImmutableMap.of(InetAddress.getLocalHost().getHostAddress(), PrestoClusterManagerUtil.NODE_UP_STATE);
        }
        else {
          // If not empty, return the master list marking all the nodes as deactivated state
          for (Map.Entry<String, String> item : allNodesMap.entrySet()) {
            item.setValue(PrestoClusterManagerUtil.NODE_DOWN_STATE);
          }

          return allNodesMap;
        }
      }

      // If any node is not present in the master list but present in nodeListFromServer,
      // then these are newly added notes. Mark them as ACTIVATED in the master list
      for (PrestoClusterManagerUtil.Stats node : nodeListFromServer) {
        if (!allNodesMap.containsKey(node.getUri().getHost())) {
          allNodesMap.put(node.getUri().getHost(), PrestoClusterManagerUtil.NODE_UP_STATE);
        }
      }

      List<PrestoClusterManagerUtil.Stats> failedNodes = PrestoClusterManagerUtil.getFailedNodes(conf);

      // If a node is present in failedNodes, then mark the node as DEACTIVATED in the master list
      for (PrestoClusterManagerUtil.Stats node : failedNodes) {
        if (!allNodesMap.containsKey(node.getUri().getHost())) {
          log.warn("Failed Node " + node.getUri().getHost() + " not present in the master list");
        }
        allNodesMap.put(node.getUri().getHost(), PrestoClusterManagerUtil.NODE_DOWN_STATE);
      }

      Set<String> nodesFromServerSet = new HashSet<String>();
      for (PrestoClusterManagerUtil.Stats item : nodeListFromServer) {
        nodesFromServerSet.add(item.getUri().getHost());
      }

      // If any node is present in the master list but not present in the latest list from the server,
      // the node is probably lost. We might have lost it in the failedNodes between two subsequent calls
      // to /v1/node/failed
      for (String key : allNodesMap.keySet()) {
        if (!nodesFromServerSet.contains(key)) {
          allNodesMap.put(key, PrestoClusterManagerUtil.NODE_DOWN_STATE);
        }
      }

      return allNodesMap;
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
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
