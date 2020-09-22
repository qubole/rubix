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
package com.qubole.rubix.spi;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.ishugaliy.allgood.consistent.hash.ConsistentHash;
import org.ishugaliy.allgood.consistent.hash.HashRing;
import org.ishugaliy.allgood.consistent.hash.hasher.DefaultHasher;
import org.ishugaliy.allgood.consistent.hash.node.SimpleNode;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * Created by stagra on 14/1/16.
 */

/*
 * This class should be implemented for each engine.
 * The implementation should return the nodes in a form which the scheduler of that engine can recognize and route the splits to
 */
public abstract class ClusterManager
{
  private static Log log = LogFactory.getLog(ClusterManager.class);

  private String currentNodeName;
  private String nodeHostname;
  private String nodeHostAddress;
  private final AtomicReference<LoadingCache<String, List<String>>> nodesCache = new AtomicReference<>();
  // Concluded from testing that Metro Hash results in better load distribution across the nodes in cluster.
  private final ConsistentHash<SimpleNode> consistentHashRing = HashRing.<SimpleNode>newBuilder()
          .hasher(DefaultHasher.METRO_HASH)
          .build();

  public abstract ClusterType getClusterType();

  /*
   * gets the nodes as per the engine
   * returns null in case node list cannot be fetched
   * returns empty in case of master-only setup
   */
  protected abstract List<String> getNodesInternal();

  protected String getCurrentNodeHostname()
  {
    return nodeHostname;
  }

  protected String getCurrentNodeHostAddress()
  {
    return nodeHostAddress;
  }

  private List<String> getNodesAndUpdateState()
  {
    requireNonNull(nodesCache, "ClusterManager used before initialization");
    List<String> nodes = getNodesInternal();
    if (nodes == null) {
      nodes = ImmutableList.of();
    } else if (nodes.isEmpty()) {
      // Empty result set => server up and only master node running, return localhost has the only node
      // Do not need to consider failed nodes list as 1node cluster and server is up since it replied to allNodesRequest
      nodes = ImmutableList.of(getCurrentNodeHostAddress());
    }

    // remove stale nodes from consistent hash ring
    for (SimpleNode ringNode : consistentHashRing.getNodes()) {
      if (!nodes.contains(ringNode.getKey()))
      {
        log.debug("Removing node: " + ringNode.getKey() + " from consistent hash ring, Total nodes: " + consistentHashRing.getNodes());
        consistentHashRing.remove(ringNode);
      }
    }

    // add new nodes to consistent hash ring
    for (String node : nodes) {
      SimpleNode ringNode = SimpleNode.of(node);
      if (!consistentHashRing.contains(ringNode)) {
        log.debug("Adding node: " + ringNode.getKey() + " to consistent hash ring, Total nodes: " + consistentHashRing.getNodes());
        consistentHashRing.add(ringNode);
      }
    }

    if (currentNodeName == null) {
      if (consistentHashRing.contains(SimpleNode.of(getCurrentNodeHostname()))) {
        currentNodeName = getCurrentNodeHostname();
      }
      else if (consistentHashRing.contains(SimpleNode.of(getCurrentNodeHostAddress()))) {
        currentNodeName = getCurrentNodeHostAddress();
      }
      else {
        log.error(String.format("Could not initialize cluster nodes=%s nodeHostName=%s nodeHostAddress=%s " +
                "currentNodeIndex=%s", nodes, getCurrentNodeHostname(), getCurrentNodeHostAddress(), currentNodeName));
      }
    }
    return nodes;
  }

  public void initialize(Configuration conf)
          throws UnknownHostException
  {
    if (nodesCache.get() == null) {
      synchronized (nodesCache) {
        if (nodesCache.get() == null) {
          int nodeRefreshTime = CacheConfig.getClusterNodeRefreshTime(conf);

          nodeHostname = InetAddress.getLocalHost().getCanonicalHostName();
          nodeHostAddress = InetAddress.getLocalHost().getHostAddress();

          ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setName("rubix-get-nodes-thread");
            t.setDaemon(true);
            return t;
          });

          nodesCache.set(
                  CacheBuilder.newBuilder()
                  .refreshAfterWrite(nodeRefreshTime, TimeUnit.SECONDS)
                  .build(CacheLoader.asyncReloading(new CacheLoader<String, List<String>>()
                  {
                    @Override
                    public List<String> load(String s)
                    {
                      return getNodesAndUpdateState();
                    }
                  }, executor)));
        }
      }
    }
  }

  public String locateKey(String key)
  {
    return consistentHashRing.locate(key).orElseThrow(() -> new RuntimeException("Unable to locate key: " + key)).getKey();
  }

  // Returns sorted list of nodes in the cluster
  public List<String> getNodes()
  {
    return nodesCache.get().getUnchecked("nodes");
  }

  public String getCurrentNodeName()
  {
    if (currentNodeName == null) {
      // getNodes() updates the currentNodeName
      List<String> nodes = getNodes();
      if (nodes == null) {
        log.error("Initialization not done for Cluster Type: " + getClusterType());
        throw new RuntimeException("Unable to find current node name");
      }
    }
    return currentNodeName;
  }
}
