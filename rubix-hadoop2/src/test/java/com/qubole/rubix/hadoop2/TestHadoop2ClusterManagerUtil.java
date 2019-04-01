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

import com.google.common.collect.Lists;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * Created by Abhishek on 7/2/18.
 */
public class TestHadoop2ClusterManagerUtil
{
  private static Log log = LogFactory.getLog(TestHadoop2ClusterManagerUtil.class);

  // The REST API endpoint used for fetching node information from the cluster.
  static final String CLUSTER_NODES_ENDPOINT = "/ws/v1/cluster/nodes";

  // The address used for testing the cluster manager.
  static final String HADOOP2_CLUSTER_ADDRESS = "localhost:45326";

  // The worker hostnames used for verifying cluster manager behaviour
  static final String WORKER_HOSTNAME_1 = "192.168.1.3";
  static final String WORKER_HOSTNAME_2 = "192.168.2.252";
  static final String WORKER_HOSTNAME_3 = "192.168.1.6";
  static final String WORKER_HOSTNAME_4 = "192.168.2.4";
  static final String WORKER_HOSTNAME_5 = "192.168.2.254";
  static final String WORKER_HOSTNAME_6 = "192.168.2.255";

  private TestHadoop2ClusterManagerUtil()
  {
  }

  /**
   * Create a server to mock a Hadoop endpoint.
   *
   * @param endpoint    The API endpoint to mock.
   * @param handler     The handler used to handle requests.
   * @return The mocked endpoint server.
   * @throws IOException if the server could not be created.
   */
  static HttpServer createServer(String endpoint, HttpHandler handler)
      throws IOException
  {
    final HttpServer server = HttpServer.create(new InetSocketAddress(45326), 0);
    server.createContext(endpoint, handler);
    server.setExecutor(null); // creates a default executor
    server.start();
    return server;
  }

  /**
   * Fetch a list of node hostnames from the specified endpoint.
   *
   * @param endpoint          The endpoint to query.
   * @param responseHandler   The handler used to return the desired response.
   * @return A list of hostnames for the nodes in the cluster.
   * @throws IOException if the cluster server could not be created.
   */
  static List<String> getNodeHostnamesFromCluster(String endpoint, HttpHandler responseHandler,
                                                  Configuration conf, ClusterType clusterType)
      throws IOException
  {
    final HttpServer server = createServer(endpoint, responseHandler);
    log.info("STARTED SERVER");

    ClusterManager clusterManager = getClusterManagerInstance(clusterType, conf);
    clusterManager.initialize(conf);
    List<String> nodes = Lists.newArrayList(clusterManager.getNodes().keySet().toArray(new String[0]));
    log.info("Got nodes: " + nodes);

    server.stop(0);
    return nodes;
  }

  static ClusterManager getClusterManagerInstance(ClusterType clusterType, Configuration conf)
      throws IOException
  {
    String clusterManagerClassName = CacheConfig.getClusterManagerClass(conf, clusterType);
    ClusterManager manager = null;

    try {
      Class clusterManagerClass = conf.getClassByName(clusterManagerClassName);
      Constructor constructor = clusterManagerClass.getConstructor();
      manager = (ClusterManager) constructor.newInstance();
    }
    catch (ClassNotFoundException | NoSuchMethodException | InstantiationException |
        IllegalAccessException | InvocationTargetException ex) {
      String errorMessage = String.format("Not able to initialize ClusterManager class : {0} ",
          clusterManagerClassName);
      log.error(errorMessage);
      throw new IOException(errorMessage, ex);
    }

    return manager;
  }
}
