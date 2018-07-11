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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

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
 * Created by sakshia on 28/7/16.
 */
public class Hadoop2ClusterManager extends ClusterManager
{
  private boolean isMaster = true;
  static LoadingCache<String, List<String>> nodesCache;
  YarnConfiguration yconf;
  private Log log = LogFactory.getLog(Hadoop2ClusterManager.class);

  @Override
  public void initialize(Configuration conf)
  {
    super.initialize(conf);
    yconf = new YarnConfiguration(conf);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    nodesCache = CacheBuilder.newBuilder()
        .refreshAfterWrite(getNodeRefreshTime(), TimeUnit.SECONDS)
        .build(CacheLoader.asyncReloading(new CacheLoader<String, List<String>>()
        {
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
              List<Hadoop2ClusterManagerUtil.Node> allNodes = Hadoop2ClusterManagerUtil.getAllNodes(yconf);
              if (allNodes == null) {
                isMaster = false;
                return ImmutableList.of();
              }

              Set<String> hosts = new HashSet<>();

              if (allNodes.isEmpty()) {
                // Empty result set => server up and only master node running, return localhost has the only node
                // Do not need to consider failed nodes list as 1node cluster and server is up since it replied to allNodesRequest
                return ImmutableList.of(InetAddress.getLocalHost().getHostAddress());
              }

              for (Hadoop2ClusterManagerUtil.Node node : allNodes) {
                String state = node.getState();
                log.debug("Hostname: " + node.getNodeHostName() + "State: " + state);
                //keep only healthy data nodes
                if (state.equalsIgnoreCase("Running") || state.equalsIgnoreCase("New") || state.equalsIgnoreCase("Rebooted")) {
                  hosts.add(node.getNodeHostName());
                }
              }

              if (hosts.isEmpty()) {
                throw new Exception("No healthy data nodes found.");
              }

              List<String> hostList = Lists.newArrayList(hosts.toArray(new String[0]));
              Collections.sort(hostList);
              log.debug("Hostlist: " + hostList.toString());
              return hostList;
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
      return nodesCache.get("nodeList");
    }
    catch (ExecutionException e) {
      e.printStackTrace();
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
    return ClusterType.HADOOP2_CLUSTER_MANAGER;
  }
}
