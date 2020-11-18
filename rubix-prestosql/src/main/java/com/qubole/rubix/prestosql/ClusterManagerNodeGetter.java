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

import io.prestosql.spi.Node;
import io.prestosql.spi.NodeManager;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/*
 * Class that encapsulates logic using NodeManager to satisfy API
 * of ClusterManager. This logic was put in a separate class so
 * that both PrestoClusterManager and SyncPrestoClusterManager
 * can share it.
 */
public class ClusterManagerNodeGetter {
  public static Set<String> getNodesInternal(NodeManager nodeManager)
  {
    requireNonNull(nodeManager, "nodeManager is null");
    return nodeManager.getWorkerNodes().stream()
        .map(Node::getHost)
        .collect(Collectors.toSet());
  }

  public static String getCurrentNodeHostname(NodeManager nodeManager) {
    requireNonNull(nodeManager, "nodeManager is null");
    return nodeManager.getCurrentNode().getHost();
  }

  public static String getCurrentNodeHostAddress(NodeManager nodeManager)
      throws UnknownHostException {
    requireNonNull(nodeManager, "nodeManager is null");
    return nodeManager.getCurrentNode().getHostAndPort().toInetAddress().getHostAddress();
  }
}
