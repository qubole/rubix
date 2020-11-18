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
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.qubole.rubix.common.utils.ClusterUtil;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.Node;
import io.prestosql.spi.NodeManager;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class StandaloneNodeManager
    implements NodeManager {
  private static Log LOG = LogFactory.getLog(StandaloneNodeManager.class);
  private static final int DEFAULT_SERVER_PORT = 8081;
  private static final String DEFAULT_USER = "rubix";
  public static final String SERVER_PORT_CONF_KEY = "caching.fs.presto-server-port";

  private final String serverAddress;
  private final Node currentNode;
  private final int serverPort;

  public StandaloneNodeManager(Configuration conf)
      throws UnknownHostException {
    this.serverPort = conf.getInt(SERVER_PORT_CONF_KEY, DEFAULT_SERVER_PORT);
    this.serverAddress = ClusterUtil.getMasterHostname(conf);
    this.currentNode = new StandaloneNode(URI.create("http://" + InetAddress.getLocalHost().getHostAddress()));
  }

  @Override
  public Set<Node> getAllNodes() {
    return getWorkerNodes();
  }

  @Override
  public Set<Node> getWorkerNodes() {
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
          LOG.warn("v1/node failed with code: " + allNodesResponseCode);
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

      Set<Node> hosts = new HashSet<Node>();
      for (Stats node : allNodes) {
        hosts.add(new StandaloneNode(node.getUri()));
      }

      return hosts;
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Node getCurrentNode() {
    return currentNode;
  }

  @Override
  public String getEnvironment() {
    return "testenv";
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

  public static class StandaloneNode
      implements Node
  {
    private final URI uri;

    public StandaloneNode(URI uri) {
      this.uri = uri;
    }

    @Override
    public String getHost() {
      return uri.getHost();
    }

    @Override
    public HostAddress getHostAndPort() {
      return HostAddress.fromUri(uri);
    }

    @Override
    public URI getHttpUri() {
      return uri;
    }

    @Override
    public String getNodeIdentifier() {
      return uri.toString();
    }

    @Override
    public String getVersion() {
      return "<unknown>";
    }

    @Override
    public boolean isCoordinator() {
      return false;
    }
  }

  private static class Stats
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
