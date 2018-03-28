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
package com.qubole.rubix.presto;

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

/**
 * Created by stagra on 14/1/16.
 */

@Test(singleThreaded = true)
public class TestClusterManager
{
  private Log log = LogFactory.getLog(TestClusterManager.class);

  @Test
  /*
   * Tests that the worker nodes returned are correctly handled by PrestoClusterManager and sorted list of hosts is returned
   */
  public void testGetNodes()
      throws IOException
  {
    HttpServer server = createServer("/v1/node", new MultipleWorkers(), "/v1/node/failed", new NoFailedNode());

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
    HttpServer server = createServer("/v1/node", new NoWorker(), "/v1/node/failed", new NoFailedNode());

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
    HttpServer server = createServer("/v1/node", new MultipleWorkers(), "/v1/node/failed", new OneFailedNode());

    log.info("STARTED SERVER");

    ClusterManager clusterManager = getPrestoClusterManager();
    List<String> nodes = clusterManager.getNodes();
    log.info("Got nodes: " + nodes);

    assertTrue(nodes.size() == 1, "Should only have two nodes");
    assertTrue(nodes.get(0).equals("192.168.2.252"), "Wrong nodes data");

    server.stop(0);
  }

  private HttpServer createServer(String endpoint1, HttpHandler handler1, String endpoint2, HttpHandler handler2)
      throws IOException
  {
    HttpServer server = HttpServer.create(new InetSocketAddress(45326), 0);
    server.createContext(endpoint1, handler1);
    server.createContext(endpoint2, handler2);
    server.setExecutor(null); // creates a default executor
    server.start();
    return server;
  }

  private ClusterManager getPrestoClusterManager()
  {
    ClusterManager clusterManager = new PrestoClusterManager();
    Configuration conf = new Configuration();
    conf.setInt(PrestoClusterManager.serverPortConf, 45326);
    clusterManager.initialize(conf);
    return clusterManager;
  }

  class MultipleWorkers implements HttpHandler
  {
    public void handle(HttpExchange exchange) throws IOException
    {
      String nodes = "[{\"uri\":\"http://192.168.2.252:8083\",\"recentRequests\":119.0027780896941,\"recentFailures\":119.00267353393015,\"recentSuccesses\":1.0845754237194612E-4,\"lastRequestTime\":\"2016-01-14T13:26:29.948Z\",\"lastResponseTime\":\"2016-01-14T13:26:29.948Z\",\"recentFailureRatio\":0.999999121400646,\"age\":\"6.68h\",\"recentFailuresByType\":{\"java.util.concurrent.TimeoutException\":2.4567611856996272E-6,\"java.net.SocketTimeoutException\":119.00237271323728,\"java.net.SocketException\":2.98363931759331E-4}},{\"uri\":\"http://192.168.1.3:8082\",\"recentRequests\":119.00277802527565,\"recentFailures\":119.00282273097419,\"recentSuccesses\":0.0,\"lastRequestTime\":\"2016-01-14T13:26:29.701Z\",\"lastResponseTime\":\"2016-01-14T13:26:29.701Z\",\"recentFailureRatio\":1.0000003756693692,\"age\":\"21.81h\",\"recentFailuresByType\":{\"java.util.concurrent.TimeoutException\":0.0,\"java.net.SocketTimeoutException\":119.00258110193407,\"java.net.ConnectException\":0.0,\"java.net.SocketException\":2.416290401318479E-4,\"java.net.NoRouteToHostException\":1.3332509542453224E-21}}]\n";
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, nodes.length());
      OutputStream os = exchange.getResponseBody();
      os.write(nodes.getBytes());
      os.close();
    }
  }

  class NoWorker implements HttpHandler
  {
    public void handle(HttpExchange exchange) throws IOException
    {
      String nodes = "[]\n";
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, nodes.length());
      OutputStream os = exchange.getResponseBody();
      os.write(nodes.getBytes());
      os.close();
    }
  }

  class NoFailedNode implements HttpHandler
  {
    public void handle(HttpExchange exchange) throws IOException
    {
      String nodes = "[]\n";
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, nodes.length());
      OutputStream os = exchange.getResponseBody();
      os.write(nodes.getBytes());
      os.close();
    }
  }

  class OneFailedNode implements HttpHandler
  {
    public void handle(HttpExchange exchange) throws IOException
    {
      String nodes = "[{\"uri\":\"http://192.168.1.3:8082\",\"recentRequests\":119.00277802527565,\"recentFailures\":119.00282273097419,\"recentSuccesses\":0.0,\"lastRequestTime\":\"2016-01-14T13:26:29.701Z\",\"lastResponseTime\":\"2016-01-14T13:26:29.701Z\",\"recentFailureRatio\":1.0000003756693692,\"age\":\"21.81h\",\"recentFailuresByType\":{\"java.util.concurrent.TimeoutException\":0.0,\"java.net.SocketTimeoutException\":119.00258110193407,\"java.net.ConnectException\":0.0,\"java.net.SocketException\":2.416290401318479E-4,\"java.net.NoRouteToHostException\":1.3332509542453224E-21}}]\n";
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, nodes.length());
      OutputStream os = exchange.getResponseBody();
      os.write(nodes.getBytes());
      os.close();
    }
  }
}
