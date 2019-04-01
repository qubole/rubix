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
package com.qubole.rubix.hadoop2;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import com.qubole.rubix.spi.thrift.NodeState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.net.InetAddress;
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
  static LoadingCache<String, Map<String, NodeState>> nodesCache;
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
        .build(CacheLoader.asyncReloading(new CacheLoader<String, Map<String, NodeState>>()
        {
          @Override
          public Map<String, NodeState> load(String s)
              throws Exception
          {
            try {
              Map<String, NodeState> hosts = new LinkedHashMap();
              List<Hadoop2ClusterManagerUtil.Node> allNodes = Hadoop2ClusterManagerUtil.getAllNodes(yconf);

              if (allNodes == null) {
                return ImmutableMap.of();
              }

              if (allNodes.isEmpty()) {
                // Empty result set => server up and only master node running, return localhost has the only node
                // Do not need to consider failed nodes list as 1node cluster and server is up since it replied to allNodesRequest
                return ImmutableMap.of(InetAddress.getLocalHost().getHostAddress(), NodeState.ACTIVE);
              }

              for (Hadoop2ClusterManagerUtil.Node node : allNodes) {
                NodeState nodeState = getNodeState(node.getState());
                hosts.put(node.getNodeHostName(), nodeState);
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

  private static NodeState getNodeState(String nodeState)
  {
    if (nodeState.equalsIgnoreCase("Running") ||
        nodeState.equalsIgnoreCase("New") ||
        nodeState.equalsIgnoreCase("Rebooted")) {
      return NodeState.ACTIVE;
    }
    else {
      return NodeState.INACTIVE;
    }
  }

  @Override
  public Map<String, NodeState> getNodes()
  {
    try {
      return nodesCache.get("nodeList");
    }
    catch (ExecutionException e) {
      log.error(Throwables.getStackTraceAsString(e));
    }

    return null;
  }

  @Override
  public ClusterType getClusterType()
  {
    return ClusterType.HADOOP2_CLUSTER_MANAGER;
  }
}
