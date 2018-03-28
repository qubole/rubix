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
package com.qubole.rubix.hadoop1;

import com.qubole.rubix.spi.ClusterManager;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Created by stagra on 9/2/16.
 */
@Test(singleThreaded = true)
public class TestClusterManager
{
  private static final Log LOG = LogFactory.getLog(TestClusterManager.class);

  @Test
  public void testGetNodes()
      throws IOException, ExecutionException
  {
    HttpServer server = startServer(true);
    ClusterManager cm = getHadoop1ClusterManager();
    List<String> nodes = cm.getNodes();

    assertTrue(cm.isMaster(), "Should have been master");
    assertTrue(nodes.size() == 4, "Should have 4 nodes");

    String[] expected = new String[]{"ip-172-31-37-232.ec2.internal", "ip-172-31-37-233.ec2.internal", "ip-172-31-37-234.ec2.internal", "ip-172-31-37-235.ec2.internal"};
    for (int i = 0; i < 4; i++) {
      assertTrue(nodes.get(i).equals(expected[i]), String.format("Wrong Node at position %d, expected %s but got %s", i, expected[i], nodes.get(i)));
    }
    server.stop(0);
  }

  @Test
  public void testIsMasterOnWorker()
      throws IOException, ExecutionException
  {
    HttpServer server = startServer(false);
    ClusterManager cm = getHadoop1ClusterManager();
    assertFalse(cm.isMaster(), "Should have been worker, isMaster=" + cm.isMaster());
    server.stop(0);
  }

  private ClusterManager getHadoop1ClusterManager()
  {
    ClusterManager clusterManager = new Hadoop1ClusterManager();
    Configuration conf = new Configuration();
    conf.setInt(Hadoop1ClusterManager.nnPortConf, 45326);
    clusterManager.initialize(conf);
    return clusterManager;
  }

  private HttpServer startServer(boolean master)
      throws IOException
  {
    HttpServer server = HttpServer.create(new InetSocketAddress(45326), 0);
    if (master) {
      server.createContext("/dfsnodelist.jsp", new MasterDFSAdminHandler());
    }
    server.setExecutor(null); // creates a default executor
    server.start();
    return server;
  }

  class MasterDFSAdminHandler
      implements HttpHandler
  {
    @Override
    public void handle(HttpExchange exchange)
        throws IOException
    {
      URL url = TestClusterManager.class.getResource("/masterResponse.html");
      File file;
      try {
        file = new File(url.toURI());
      }
      catch (URISyntaxException e) {
        throw new IOException(e);
      }
      InputStream is = url.openStream();
      byte[] data = new byte[(int) file.length()];
      int len = is.read(data);
      is.close();

      //String response = new String(data, "UTF-8");
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, len);
      OutputStream os = exchange.getResponseBody();
      os.write(data);
      os.close();
    }
  }
}
