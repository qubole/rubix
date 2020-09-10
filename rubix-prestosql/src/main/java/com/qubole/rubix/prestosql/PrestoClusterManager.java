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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.qubole.rubix.common.utils.ClusterUtil;
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import io.prestosql.spi.Node;
import io.prestosql.spi.NodeManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Created by stagra on 14/1/16.
 */
public class PrestoClusterManager extends ClusterManager
{
  private static final String DEFAULT_USER = "qubole";
  private int serverPort = 8081;
  private String serverAddress = "localhost";

  private Log log = LogFactory.getLog(PrestoClusterManager.class);

  @Nullable
  private static volatile NodeManager nodeManager;
  public static String serverPortConf = "caching.fs.presto-server-port";

  // Safe to use single instance of HttpClient since Supplier.get() provides synchronization
  @Override
  public void initialize(Configuration conf)
          throws UnknownHostException
  {
    super.initialize(conf);
    this.serverPort = conf.getInt(serverPortConf, serverPort);
    this.serverAddress = ClusterUtil.getMasterHostname(conf);
  }

  @Override
  public List<String> getNodesInternal()
  {
    if (nodeManager != null) {
      return getNodesFromNodeManager();
    }

    try {
      URL allNodesRequest = getNodeUrl();
      URL failedNodesRequest = getFailedNodeUrl();

      HttpURLConnection allHttpCon = getHttpURLConnection(allNodesRequest);

      int allNodesResponseCode = allHttpCon.getResponseCode();

      StringBuilder allResponse = new StringBuilder();
      StringBuilder failedResponse = new StringBuilder();
      try {
        if (allNodesResponseCode == HttpURLConnection.HTTP_OK) {
          BufferedReader in = new BufferedReader(new InputStreamReader(allHttpCon.getInputStream()));
          String inputLine = "";
          try {
            while ((inputLine = in.readLine()) != null) {
              allResponse.append(inputLine);
            }
          }
          catch (IOException e) {
            throw new IOException(e);
          }
          finally {
            in.close();
          }
        }
        else {
          log.warn("v1/node failed with code: " + allNodesResponseCode);
          return null;
        }
      }
      catch (IOException e) {
        throw new IOException(e);
      }
      finally {
        allHttpCon.disconnect();
      }

      HttpURLConnection failHttpConn = getHttpURLConnection(failedNodesRequest);
      int failedNodesResponseCode = failHttpConn.getResponseCode();
      // check on failed nodes
      try {
        if (failedNodesResponseCode == HttpURLConnection.HTTP_OK) {
          BufferedReader in = new BufferedReader(new InputStreamReader(failHttpConn.getInputStream()));
          String inputLine;
          try {
            while ((inputLine = in.readLine()) != null) {
              failedResponse.append(inputLine);
            }
          }
          catch (IOException e) {
            throw new IOException(e);
          }
          finally {
            in.close();
          }
        }
      }
      catch (IOException e) {
        throw new IOException(e);
      }
      finally {
        failHttpConn.disconnect();
      }

      Gson gson = new Gson();
      Type type = new TypeToken<List<Stats>>()
      {
      }.getType();

      List<Stats> allNodes = gson.fromJson(allResponse.toString(), type);
      List<Stats> failedNodes = gson.fromJson(failedResponse.toString(), type);

      if (failedNodes.isEmpty()) {
        failedNodes = ImmutableList.of();
      }

      // keep only the healthy nodes
      allNodes.removeAll(failedNodes);

      Set<String> hosts = new HashSet<String>();
      for (Stats node : allNodes) {
        hosts.add(node.getUri().getHost());
      }

      return Lists.newArrayList(hosts.toArray(new String[0]));
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private HttpURLConnection getHttpURLConnection(URL urlRequest)
          throws IOException
  {
    requireNonNull(urlRequest, "urlRequest is null");
    HttpURLConnection allHttpCon = (HttpURLConnection) urlRequest.openConnection();
    allHttpCon.setConnectTimeout(500); //ms
    allHttpCon.setRequestMethod("GET");
    allHttpCon.setRequestProperty("X-Presto-User", DEFAULT_USER);
    return allHttpCon;
  }

  private List<String> getNodesFromNodeManager()
  {
    requireNonNull(nodeManager, "nodeManager is null");
    List<String> workers = nodeManager.getWorkerNodes().stream()
            .filter(node -> !node.isCoordinator())
            .map(Node::getHost)
            .collect(Collectors.toList());

    return workers;
  }

  @Override
  protected String getCurrentNodeHostname()
  {
    if (nodeManager != null) {
      return nodeManager.getCurrentNode().getHost();
    }

    return super.getCurrentNodeHostname();
  }

  @Override
  protected String getCurrentNodeHostAddress()
  {
    if (nodeManager != null) {
      try {
        return nodeManager.getCurrentNode().getHostAndPort().toInetAddress().getHostAddress();
      }
      catch (UnknownHostException e) {
        log.warn("Could not get HostAddress from NodeManager", e);
        // fallback
      }
    }

    return super.getCurrentNodeHostAddress();
  }

  @Override
  public ClusterType getClusterType()
  {
    return ClusterType.PRESTOSQL_CLUSTER_MANAGER;
  }

  public static void setPrestoServerPort(Configuration conf, int port)
  {
    conf.setInt(serverPortConf, port);
  }

  public static void setNodeManager(NodeManager nodeManager)
  {
    PrestoClusterManager.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
  }

  private URL getNodeUrl()
          throws MalformedURLException
  {
    return new URL("http://" + serverAddress + ":" + serverPort + "/v1/node");
  }

  private URL getFailedNodeUrl()
          throws MalformedURLException
  {
    return new URL("http://" + serverAddress + ":" + serverPort + "/v1/node/failed");
  }

  public static class Stats
  {
    URI uri;
    String lastResponseTime;

    public Stats()
    {
    }

    public Stats(URI uri, String lastResponseTime)
    {
      this.uri = uri;
      this.lastResponseTime = lastResponseTime;
    }

    public URI getUri()
    {
      return uri;
    }

    public void setURI(URI uri)
    {
      this.uri = uri;
    }

    String getLastResponseTime()
    {
      return lastResponseTime;
    }

    public void setLastResponseTime(String lastResponseTime)
    {
      this.lastResponseTime = lastResponseTime;
    }

    @Override
    public boolean equals(Object other)
    {
      if (this == other) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }
      Stats o = (Stats) other;

      if (!uri.equals(o.getUri())) {
        return false;
      }

      if (lastResponseTime != null && o.getLastResponseTime() != null) {
        return lastResponseTime.equals(o.getLastResponseTime());
      }

      return lastResponseTime == o.getLastResponseTime();
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(uri, lastResponseTime);
    }
  }
}
