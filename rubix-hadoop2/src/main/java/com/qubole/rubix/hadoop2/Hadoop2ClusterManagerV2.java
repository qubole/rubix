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
package com.qubole.rubix.hadoop2;

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
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Abhishek on 7/2/18.
 */
public class Hadoop2ClusterManagerV2 extends ClusterManager
{
  private boolean isMaster = true;
  static LoadingCache<String, Map<String, String>> nodesCache;
  YarnConfiguration yconf;
  private Log log = LogFactory.getLog(Hadoop2ClusterManagerV2.class);

  @Override
  public void initialize(Configuration conf)
  {
    super.initialize(conf);
    yconf = new YarnConfiguration(conf);
    ExecutorService executor = Executors.newSingleThreadExecutor();

    nodesCache = CacheBuilder.newBuilder()
        .refreshAfterWrite(getNodeRefreshTime(), TimeUnit.SECONDS)
        .build(CacheLoader.asyncReloading(new CacheLoader<String, Map<String, String>>()
        {
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
              Map<String, String> hosts = new LinkedHashMap<String, String>();
              List<Hadoop2ClusterManagerUtil.Node> allNodes = Hadoop2ClusterManagerUtil.getAllNodes(yconf);

              if (allNodes == null) {
                isMaster = false;
                return ImmutableMap.of();
              }

              if (allNodes.isEmpty()) {
                // Empty result set => server up and only master node running, return localhost has the only node
                // Do not need to consider failed nodes list as 1node cluster and server is up since it replied to allNodesRequest
                return ImmutableMap.of(InetAddress.getLocalHost().getHostAddress(), "RUNNING");
              }

              for (Hadoop2ClusterManagerUtil.Node node : allNodes) {
                String state = node.getState();
                log.debug("Hostname: " + node.getNodeHostName() + "State: " + state);
                hosts.put(node.getNodeHostName(), node.getState());
              }
              log.debug("Hostlist: " + hosts.toString());
              return hosts;
            }
            catch (Exception e) {
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

  @Override
  public List<String> getNodes()
  {
    try {
      return new ArrayList<>(nodesCache.get("nodeList").keySet());
    }
    catch (ExecutionException e) {
      e.printStackTrace();
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
        if (nodeState.equalsIgnoreCase("Running") || nodeState.equalsIgnoreCase("New")
            || nodeState.equalsIgnoreCase("Rebooted")) {
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
        if (nodeState.equalsIgnoreCase("Running") || nodeState.equalsIgnoreCase("New")
            || nodeState.equalsIgnoreCase("Rebooted")) {
          return i;
        }
      }

      for (int i = nodeList.size() - 1; i > startIndex; i--) {
        String nodeState = nodesCache.get("nodeList").get(nodeList.get(i));
        if (nodeState.equalsIgnoreCase("Running") || nodeState.equalsIgnoreCase("New")
            || nodeState.equalsIgnoreCase("Rebooted")) {
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
    return ClusterType.HADOOP2_CLUSTER_MANAGER;
  }
}
