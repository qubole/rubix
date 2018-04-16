/**
 * Copyright (c) 2016. Qubole Inc
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

import com.qubole.rubix.spi.ClusterManager;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestClusterManager
{
  private Log log = LogFactory.getLog(TestClusterManager.class);

  // The REST API endpoint used for fetching node information from the cluster.
  private static final String CLUSTER_NODES_ENDPOINT = "/ws/v1/cluster/nodes";

  // The address used for testing the cluster manager.
  private static final String HADOOP2_CLUSTER_ADDRESS = "localhost:45326";

  // The worker hostnames used for verifying cluster manager behaviour
  private static final String WORKER_HOSTNAME_1 = "192.168.1.3";
  private static final String WORKER_HOSTNAME_2 = "192.168.2.252";

  @Test
  /*
   * Tests that the worker nodes returned are correctly handled by HadoopClusterManager and sorted list of hosts is returned
   */
  public void testGetNodes()
      throws IOException
  {
    final HttpServer server = createServer(CLUSTER_NODES_ENDPOINT, new MultipleWorkers());

    log.info("STARTED SERVER");

    final ClusterManager clusterManager = buildHadoop2ClusterManager();
    final List<String> nodes = clusterManager.getNodes();
    log.info("Got nodes: " + nodes);

    assertTrue(nodes.size() == 2, "Should only have two nodes");
    assertTrue(nodes.get(0).equals(WORKER_HOSTNAME_1) && nodes.get(1).equals(WORKER_HOSTNAME_2), "Wrong nodes data");

    server.stop(0);
  }

  @Test
  /*
   * Tests that in a single node cluster, master node is returned as worker
   */
  public void testMasterOnlyCluster()
      throws IOException
  {
    final HttpServer server = createServer(CLUSTER_NODES_ENDPOINT, new NoWorker());

    log.info("STARTED SERVER");

    final ClusterManager clusterManager = buildHadoop2ClusterManager();
    final List<String> nodes = clusterManager.getNodes();
    log.info("Got nodes: " + nodes);

    assertTrue(nodes.size() == 1, "Should have added localhost in list");
    assertTrue(nodes.get(0).equals(InetAddress.getLocalHost().getHostAddress()), "Not added right hostname");
    server.stop(0);
  }

  @Test
  /*
   * Tests that in a cluster with unhealthy node, unhealthy node is not returned
   */
  public void testUnhealthyNodeCluster()
      throws IOException
  {
    final HttpServer server = createServer(CLUSTER_NODES_ENDPOINT, new OneUnhealthyWorker());

    log.info("STARTED SERVER");

    final ClusterManager clusterManager = buildHadoop2ClusterManager();
    final List<String> nodes = clusterManager.getNodes();
    log.info("Got nodes: " + nodes);

    assertTrue(nodes.size() == 1, "Should only have one node");
    assertTrue(nodes.get(0).equals(WORKER_HOSTNAME_2), "Wrong nodes data");

    server.stop(0);
  }

  /**
   * Initializes a {@link Hadoop2ClusterManager} for testing.
   *
   * @return The cluster manager instance to be tested.
   */
  private ClusterManager buildHadoop2ClusterManager()
  {
    final ClusterManager clusterManager = new Hadoop2ClusterManager();
    final Configuration conf = new Configuration();
    conf.set(Hadoop2ClusterManager.addressConf, HADOOP2_CLUSTER_ADDRESS);
    clusterManager.initialize(conf);
    return clusterManager;
  }

  /**
   * Create a server to mock a Hadoop endpoint.
   *
   * @param endpoint    The API endpoint to mock.
   * @param handler     The handler used to handle requests.
   * @return The mocked endpoint server.
   * @throws IOException if the server could not be created.
   */
  private HttpServer createServer(String endpoint, HttpHandler handler)
      throws IOException
  {
    final HttpServer server = HttpServer.create(new InetSocketAddress(45326), 0);
    server.createContext(endpoint, handler);
    server.setExecutor(null); // creates a default executor
    server.start();
    return server;
  }

  /**
   * Http response handler base class.
   */
  private class TestWorker implements HttpHandler
  {
    private String nodeJson;

    public TestWorker(String nodeJson)
    {
      this.nodeJson = nodeJson;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException
    {
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, nodeJson.length());
      final OutputStream os = exchange.getResponseBody();
      os.write(nodeJson.getBytes());
      os.close();
    }
  }

  /**
   * Http response handler to represent a cluster with multiple worker nodes.
   */
  class MultipleWorkers extends TestWorker
  {
    public MultipleWorkers()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"},{\"nodeHostName\":\"" + WORKER_HOSTNAME_1 + "\",\"state\":\"RUNNING\"}]}}\n");
    }
  }

  /**
   * Http response handler to represent a cluster with no worker nodes.
   */
  class NoWorker extends TestWorker
  {
    public NoWorker()
    {
      super("{\"nodes\":{\"node\":[]}}\n");
    }
  }

  /**
   * Http response handler to represent a cluster with one unhealthy worker node.
   */
  class OneUnhealthyWorker extends TestWorker
  {
    public OneUnhealthyWorker()
    {
      super("{\"nodes\":{\"node\":[{\"nodeHostName\":\"" + WORKER_HOSTNAME_2 + "\",\"state\":\"RUNNING\"},{\"nodeHostName\":\"" + WORKER_HOSTNAME_1 + "\",\"state\":\"UNHEALTHY\"}]}}\n");
    }
  }
}
