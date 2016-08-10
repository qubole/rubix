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

import com.qubole.rubix.core.ClusterManager;
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

import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

/**
 * Created by sakshia on 1/8/16.
 */
@Test(singleThreaded = true)
public class TestClusterManager
{
    private static final Log LOG = LogFactory.getLog(TestClusterManager.class);

    //Returns only healthy data nodes (Not master)
    @Test
    public void testGetNodes()
            throws IOException
    {
        HttpServer server = startServer(new MultipleNodeHandler());
        LOG.debug("Started Server");
        ClusterManager cm = getHadoop2ClusterManager();
        List<String> nodes = cm.getNodes();
        LOG.debug("Got nodes: " + nodes);
        assertTrue("Should have 2 nodes", nodes.size() == 2);
        server.stop(0);
    }


    @Test
    public void testFailOnAllNodesUnhealthy()
            throws IOException
    {
        HttpServer server = startServer(new FailedNodeHandler());
        LOG.debug("Started Server");
        try{
            ClusterManager cm = getHadoop2ClusterManager();
            List<String> nodes = cm.getNodes();
            fail("Expected exception : No healthy data nodes found.");
        }
        catch (Exception e){
            LOG.debug("Caught Exception: " + e.getMessage());
        }

        server.stop(0);
    }


    private ClusterManager getHadoop2ClusterManager()
            throws IOException
    {

            ClusterManager clusterManager = new Hadoop2ClusterManager();
            Configuration conf = new Configuration();
            conf.set(Hadoop2ClusterManager.addressConf, "localhost:45326");
            clusterManager.initialize(conf);
            return clusterManager;
    }

    private HttpServer startServer(HttpHandler handler)
            throws IOException
    {

        HttpServer server = HttpServer.create(new InetSocketAddress(45326), 0);
        server.createContext("/ws/v1/cluster/nodes", handler);
        server.setExecutor(null); // creates a default executor
        server.start();
        return server;
    }

    class MultipleNodeHandler implements HttpHandler
    {
        public void handle(HttpExchange exchange) throws IOException {
            String nodes = String.format("{\"nodes\":{\"node\":[{\"rack\":\"\\/default-rack\",\"state\":\"NEW\",\"id\":\"h2:1235\",\"nodeHostName\":\"h2\",\"nodeHTTPAddress\":\"h2:2\",\"healthStatus\":\"Unhealthy\",\"lastHealthUpdate\":1324056895432,\"healthReport\":\"Healthy\",\"numContainers\":0,\"usedMemoryMB\":0,\"availMemoryMB\":8192,\"usedVirtualCores\":0,\"availableVirtualCores\":8},{\"rack\":\"\\/default-rack\",\"state\":\"NEW\",\"id\":\"h2:1235\",\"nodeHostName\":\"%s\",\"nodeHTTPAddress\":\"h2:2\",\"healthStatus\":\"Healthy\",\"lastHealthUpdate\":1324056895432,\"healthReport\":\"Healthy\",\"numContainers\":0,\"usedMemoryMB\":0,\"availMemoryMB\":8192,\"usedVirtualCores\":0,\"availableVirtualCores\":8},{\"rack\":\"\\/default-rack\",\"state\":\"Unhealthy\",\"id\":\"h1:1234\",\"nodeHostName\":\"h1\",\"nodeHTTPAddress\":\"h1:2\",\"healthStatus\":\"Healthy\",\"lastHealthUpdate\":1324056895092,\"healthReport\":\"Healthy\",\"numContainers\":0,\"usedMemoryMB\":0,\"availMemoryMB\":8192,\"usedVirtualCores\":0,\"availableVirtualCores\":8}]}}",InetAddress.getLocalHost().getHostName());
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, nodes.length());
            OutputStream os = exchange.getResponseBody();
            os.write(nodes.getBytes());
            os.close();
        }
    }


    class FailedNodeHandler implements HttpHandler
    {
        public void handle(HttpExchange exchange) throws IOException {
            String nodes = String.format("{\"nodes\":{\"node\":[{\rack\":\"\\/default-rack\",\"state\":\"Unhealthy\",\"id\":\"h2:1235\",\"nodeHostName\":\"%s\",\"nodeHTTPAddress\":\"h2:2\",\"healthStatus\":\"Healthy\",\"lastHealthUpdate\":1324056895432,\"healthReport\":\"Healthy\",\"numContainers\":0,\"usedMemoryMB\":0,\"availMemoryMB\":8192,\"usedVirtualCores\":0,\"availableVirtualCores\":8},{\"rack\":\"\\/default-rack\",\"state\":\"Unhealthy\",\"id\":\"h1:1234\",\"nodeHostName\":\"h1\",\"nodeHTTPAddress\":\"h1:2\",\"healthStatus\":\"Healthy\",\"lastHealthUpdate\":1324056895092,\"healthReport\":\"Healthy\",\"numContainers\":0,\"usedMemoryMB\":0,\"availMemoryMB\":8192,\"usedVirtualCores\":0,\"availableVirtualCores\":8}]}}",InetAddress.getLocalHost().getHostName());
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, nodes.length());
            OutputStream os = exchange.getResponseBody();
            os.write(nodes.getBytes());
            os.close();
        }
    }


}

