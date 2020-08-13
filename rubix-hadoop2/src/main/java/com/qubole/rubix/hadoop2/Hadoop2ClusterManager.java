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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by sakshia on 28/7/16.
 */
public class Hadoop2ClusterManager extends ClusterManager
{
  YarnConfiguration yconf;
  private Log log = LogFactory.getLog(Hadoop2ClusterManager.class);

  @Override
  public void initialize(Configuration conf)
          throws UnknownHostException
  {
    super.initialize(conf);
    yconf = new YarnConfiguration(conf);
  }

  @Override
  public List<String> getNodesInternal()
  {
    try {
      List<Hadoop2ClusterManagerUtil.Node> allNodes = Hadoop2ClusterManagerUtil.getAllNodes(yconf);
      if (allNodes == null) {
        return null;
      }

      if (allNodes.isEmpty()) {
        return ImmutableList.of();
      }

      Set<String> hosts = new HashSet<>();
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
      return hostList;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public ClusterType getClusterType()
  {
    return ClusterType.HADOOP2_CLUSTER_MANAGER;
  }
}
