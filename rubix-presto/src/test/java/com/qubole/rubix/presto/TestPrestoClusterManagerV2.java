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

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import static org.testng.Assert.assertTrue;

/**
 * Created by Abhishek on 7/19/18.
 */
@Test(singleThreaded = true)
public class TestPrestoClusterManagerV2
{
  private Log log = LogFactory.getLog(TestPrestoClusterManagerV2.class);
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
  public void testLoadNodesCache_FirstSingleWorker_ThenTwoWorkers_NoFailures() throws Exception
  {
    PrestoClusterManagerV2 manager = getPrestoClusterManager();

    HttpServer server = createServer("/v1/node", worker.new SingleWorker(), "/v1/node/failed", worker.new NoFailedNode());
    Map<String, String> nodesMap = manager.loadNodesCache(conf);
    server.stop(0);

    assertTrue(nodesMap.size() == 1, "There should be only one node");
    assertTrue(nodesMap.containsValue(PrestoClusterManagerUtil.NODE_UP_STATE), "The node should be in Activated state");

    server = createServer("/v1/node", worker.new MultipleWorkers(), "/v1/node/failed", worker.new NoFailedNode());
    nodesMap = manager.loadNodesCache(conf);
    server.stop(0);

    assertTrue(nodesMap.size() == 2, "There should be two nodes");
    assertTrue(!nodesMap.containsValue(PrestoClusterManagerUtil.NODE_DOWN_STATE), "All the nodes should be in Activated state");
  }

  @Test
  public void testLoadNodesCache_FirstSingleWorker_ThenTwoWorkers_ThenOneFailure() throws Exception
  {
    PrestoClusterManagerV2 manager = getPrestoClusterManager();

    HttpServer server = createServer("/v1/node", worker.new SingleWorker(), "/v1/node/failed", worker.new NoFailedNode());
    Map<String, String> nodesMap = manager.loadNodesCache(conf);
    server.stop(0);

    assertTrue(nodesMap.size() == 1, "There should be only one node");
    assertTrue(nodesMap.containsValue(PrestoClusterManagerUtil.NODE_UP_STATE), "The node should be in Activated state");

    server = createServer("/v1/node", worker.new MultipleWorkers(), "/v1/node/failed", worker.new NoFailedNode());
    nodesMap = manager.loadNodesCache(conf);
    server.stop(0);

    assertTrue(nodesMap.size() == 2, "There should be two nodes");
    assertTrue(!nodesMap.containsValue(PrestoClusterManagerUtil.NODE_DOWN_STATE), "The nodes should be in Activated state");

    server = createServer("/v1/node", worker.new MultipleWorkers(), "/v1/node/failed", worker.new OneFailedNode());
    nodesMap = manager.loadNodesCache(conf);
    server.stop(0);

    assertTrue(nodesMap.size() == 2, "There should be two nodes");
    int activatedNodes = 0;
    int deactivatedNodes = 0;

    for (Map.Entry<String, String> item : nodesMap.entrySet()) {
      if (item.getValue().equals(PrestoClusterManagerUtil.NODE_UP_STATE)) {
        deactivatedNodes++;
      }
      else {
        activatedNodes++;
      }
    }

    assertTrue(activatedNodes == 1, "There should be one activated node");
    assertTrue(deactivatedNodes == 1, "There should be one deactivated node");
  }

  private PrestoClusterManagerV2 getPrestoClusterManager()
  {
    PrestoClusterManagerV2 clusterManager = new PrestoClusterManagerV2();
    clusterManager.initialize(conf);
    return clusterManager;
  }
}
