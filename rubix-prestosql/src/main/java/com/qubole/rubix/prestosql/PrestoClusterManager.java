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
package com.qubole.rubix.prestosql;

import com.qubole.rubix.spi.ClusterType;
import com.qubole.rubix.spi.SyncClusterManager;
import io.prestosql.spi.Node;
import io.prestosql.spi.NodeManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.UnknownHostException;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Created by stagra on 14/1/16.
 */
public class PrestoClusterManager extends SyncClusterManager
{
  private static Log log = LogFactory.getLog(PrestoClusterManager.class);
  private static volatile NodeManager nodeManager;
  private volatile Set<Node> workerNodes;

  public static void setNodeManager(NodeManager nodeManager)
  {
    PrestoClusterManager.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
  }

  @Override
  protected boolean hasStateChanged() {
    requireNonNull(nodeManager, "nodeManager is null");
    Set<Node> workerNodes = nodeManager.getWorkerNodes();
    boolean hasChanged = !workerNodes.equals(this.workerNodes);
    this.workerNodes = workerNodes;
    return hasChanged;
  }

  @Override
  public Set<String> getNodesInternal()
  {
    requireNonNull(nodeManager, "nodeManager is null");
    return nodeManager.getWorkerNodes().stream()
        .map(Node::getHost)
        .collect(Collectors.toSet());
  }

  @Override
  protected String getCurrentNodeHostname()
  {
    requireNonNull(nodeManager, "nodeManager is null");
    return nodeManager.getCurrentNode().getHost();
  }

  @Override
  protected String getCurrentNodeHostAddress()
  {
    requireNonNull(nodeManager, "nodeManager is null");
    try {
      return nodeManager.getCurrentNode().getHostAndPort().toInetAddress().getHostAddress();
    }
    catch (UnknownHostException e) {
      log.warn("Could not get HostAddress from NodeManager", e);
    }

    return super.getCurrentNodeHostAddress();
  }

  @Override
  public ClusterType getClusterType()
  {
    return ClusterType.PRESTOSQL_CLUSTER_MANAGER;
  }
}
