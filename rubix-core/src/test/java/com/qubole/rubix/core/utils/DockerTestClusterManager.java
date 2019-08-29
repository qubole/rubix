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
package com.qubole.rubix.core.utils;

import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import com.qubole.rubix.spi.thrift.ClusterNode;
import com.qubole.rubix.spi.thrift.NodeState;

import java.util.ArrayList;
import java.util.List;

public class DockerTestClusterManager extends ClusterManager
{
  @Override
  public List<ClusterNode> getNodes()
  {
    List<ClusterNode> nodeList = new ArrayList<>();
    nodeList.add(new ClusterNode("172.18.8.1", NodeState.ACTIVE));
    nodeList.add(new ClusterNode("172.18.8.2", NodeState.ACTIVE));

    return nodeList;
  }

  @Override
  public ClusterType getClusterType()
  {
    return ClusterType.TEST_CLUSTER_MANAGER;
  }
}
