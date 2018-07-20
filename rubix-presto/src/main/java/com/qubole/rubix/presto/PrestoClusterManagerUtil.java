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

import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
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
import java.util.List;
import java.util.Objects;

/**
 * Created by Abhishek on 7/18/18.
 */
public class PrestoClusterManagerUtil
{
  private PrestoClusterManagerUtil()
  {
  }

  private static Log log = LogFactory.getLog(PrestoClusterManagerUtil.class);

  private static int serverPort = 8081;
  private static String serverAddress = "localhost";
  public static String serverPortConf = "caching.fs.presto-server-port";
  public static String serverAddressConf = "master.hostname";
  public static String yarnServerAddressConf = "yarn.resourcemanager.address";

  public static final String NODE_UP_STATE = "ACTIVATED";
  public static final String NODE_DOWN_STATE = "DEACTIVATED";

  public static List<Stats> getAllNodes(Configuration conf) throws Exception
  {
    URL allNodesRequest = getNodeUrl(conf);

    HttpURLConnection allHttpCon = (HttpURLConnection) allNodesRequest.openConnection();
    allHttpCon.setConnectTimeout(500); //ms
    allHttpCon.setRequestMethod("GET");

    int allNodesResponseCode = allHttpCon.getResponseCode();
    StringBuffer allResponse = new StringBuffer();

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
        log.info(String.format("v1/node failed with code: setting this node as worker "));
        return null;
      }
    }
    catch (IOException e) {
      throw new IOException(e);
    }
    finally {
      allHttpCon.disconnect();
    }

    Gson gson = new Gson();
    Type type = new TypeToken<List<Stats>>() {
    }.getType();

    List<Stats> allNodes = gson.fromJson(allResponse.toString(), type);

    return allNodes;
  }

  public static List<Stats> getFailedNodes(Configuration conf) throws Exception
  {
    URL failedNodesRequest = getFailedNodeUrl(conf);

    HttpURLConnection failHttpConn = (HttpURLConnection) failedNodesRequest.openConnection();
    failHttpConn.setConnectTimeout(500);    //ms
    failHttpConn.setRequestMethod("GET");

    int failedNodesResponseCode = failHttpConn.getResponseCode();
    StringBuffer failedResponse = new StringBuffer();

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
    Type type = new TypeToken<List<Stats>>() {
    }.getType();
    List<Stats> failedNodes = gson.fromJson(failedResponse.toString(), type);

    if (failedNodes.isEmpty()) {
      failedNodes = ImmutableList.of();
    }

    return failedNodes;
  }

  private static String getMasterHostname(Configuration conf)
  {
    // TODO move to common place (used in HeartbeatService)
    String host;
    log.debug("Trying master.hostname");
    host = conf.get(serverAddressConf);
    if (host != null) {
      return host;
    }
    log.debug("Trying yarn.resourcemanager.address");
    host = conf.get(yarnServerAddressConf);
    if (host != null) {
      host = host.substring(0, host.indexOf(":"));
      return host;
    }
    log.debug("No hostname found in etc/*-site.xml, returning localhost");
    return serverAddress;
  }

  private static URL getNodeUrl(Configuration conf) throws MalformedURLException
  {
    int port = conf.getInt(serverPortConf, serverPort);
    String address = getMasterHostname(conf);
    return new URL("http://" + address + ":" + port + "/v1/node");
  }

  private static URL getFailedNodeUrl(Configuration conf) throws MalformedURLException
  {
    int port = conf.getInt(serverPortConf, serverPort);
    String address = getMasterHostname(conf);
    return new URL("http://" + address + ":" + port + "/v1/node/failed");
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
