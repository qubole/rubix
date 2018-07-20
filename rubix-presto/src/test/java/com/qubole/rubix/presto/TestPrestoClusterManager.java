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

import com.qubole.rubix.spi.ClusterManager;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

import static org.testng.Assert.assertTrue;

/**
 * Created by stagra on 14/1/16.
 */

@Test(singleThreaded = true)
public class TestPrestoClusterManager
{
  private Log log = LogFactory.getLog(TestPrestoClusterManager.class);
  private static TestWorker worker;
  private static Configuration conf;

  @BeforeClass
  public static void setupClass() throws IOException
  {
    worker = new TestWorker();
    conf = new Configuration();
    conf.setInt(PrestoClusterManagerUtil.serverPortConf, 45326);
  }

  private static HttpServer createServer(String endpoint1, HttpHandler handler1,
                                         String endpoint2, HttpHandler handler2) throws IOException
  {
    HttpServer server = HttpServer.create(new InetSocketAddress(45326), 0);
    server.createContext(endpoint1, handler1);
    server.createContext(endpoint2, handler2);
    server.setExecutor(null); // creates a default executor
    server.start();
    return server;
  }

  @Test
  /*
   * Tests that the worker nodes returned are correctly handled by PrestoClusterManager and sorted list of hosts is returned
   */
  public void testGetNodes()
      throws IOException
  {
    HttpServer server = createServer("/v1/node", worker.new MultipleWorkers(), "/v1/node/failed", worker.new NoFailedNode());

    log.info("STARTED SERVER");

    ClusterManager clusterManager = getPrestoClusterManager();
    List<String> nodes = clusterManager.getNodes();
    log.info("Got nodes: " + nodes);

    assertTrue(nodes.size() == 2, "Should only have two nodes");
    assertTrue(nodes.get(0).equals("192.168.1.3") && nodes.get(1).equals("192.168.2.252"), "Wrong nodes data");

    server.stop(0);
  }

  @Test
  /*
   * Tests that in a single node cluster, master node is returned as worker
   */
  public void testMasterOnlyCluster()
      throws IOException
  {
    HttpServer server = createServer("/v1/node", worker.new NoWorker(), "/v1/node/failed", worker.new NoFailedNode());

    log.info("STARTED SERVER");

    ClusterManager clusterManager = getPrestoClusterManager();
    List<String> nodes = clusterManager.getNodes();
    log.info("Got nodes: " + nodes);

    assertTrue(nodes.size() == 1, "Should have added localhost in list");
    assertTrue(nodes.get(0).equals(InetAddress.getLocalHost().getHostAddress()), "Not added right hostname");
    server.stop(0);
  }

  @Test
  /*
   * Tests that in a cluster with failed node, failed node is not returned
   */
  public void testFailedNodeCluster()
      throws IOException
  {
    HttpServer server = createServer("/v1/node", worker.new MultipleWorkers(), "/v1/node/failed", worker.new OneFailedNode());

    log.info("STARTED SERVER");

    ClusterManager clusterManager = getPrestoClusterManager();
    List<String> nodes = clusterManager.getNodes();
    log.info("Got nodes: " + nodes);

    assertTrue(nodes.size() == 1, "Should only have two nodes");
    assertTrue(nodes.get(0).equals("192.168.2.252"), "Wrong nodes data");

    server.stop(0);
  }

  private ClusterManager getPrestoClusterManager()
  {
    ClusterManager clusterManager = new PrestoClusterManager();
    clusterManager.initialize(conf);
    return clusterManager;
  }
}
