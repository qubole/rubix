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

package com.qubole.rubix.hadoop2;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Abhishek on 7/2/18.
 */
public class Hadoop2ClusterManagerUtil
{
  static String addressConf = "yarn.resourcemanager.webapp.address";
  static String localHostAddress = "localhost:8088";
  static Log log = LogFactory.getLog(Hadoop2ClusterManagerUtil.class);

  private Hadoop2ClusterManagerUtil()
  {
  }

  public static URL getNodeURL(YarnConfiguration yConf) throws MalformedURLException
  {
    String address = yConf.get(addressConf, localHostAddress);
    String serverAddress = address.substring(0, address.indexOf(":"));
    int serverPort = Integer.parseInt(address.substring(address.indexOf(":") + 1));

    return new URL("http://" + serverAddress + ":" + serverPort + "/ws/v1/cluster/nodes");
  }

  public static List<Node> getAllNodes(YarnConfiguration yConf) throws Exception
  {
    List<Node> allNodes = new ArrayList<Node>();

    StringBuffer response = new StringBuffer();
    URL obj = getNodeURL(yConf);
    HttpURLConnection httpcon = (HttpURLConnection) obj.openConnection();
    httpcon.setRequestMethod("GET");
    log.debug("Sending 'GET' request to URL: " + obj.toString());
    int responseCode = httpcon.getResponseCode();

    if (responseCode == HttpURLConnection.HTTP_OK) {
      BufferedReader in = new BufferedReader(new InputStreamReader(httpcon.getInputStream()));
      String inputLine;
      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
      in.close();
      httpcon.disconnect();
    }
    else {
      log.info("/ws/v1/cluster/nodes failed due to " + responseCode + ". Setting this node as worker.");
      httpcon.disconnect();
      return null;
    }

    Gson gson = new Gson();
    Type type = new TypeToken<NodesResponse>() {
    }.getType();
    NodesResponse nodesResponse = gson.fromJson(response.toString(), type);
    allNodes = nodesResponse.getNodes().getNodeList();

    return allNodes;
  }

  public static class NodesResponse
  {
    // Argument-less constructor is needed for GSON parsing
    public NodesResponse()
    {
    }

    @SerializedName("nodes")
    private Nodes nodes;

    // Necessary for GSON parsing
    public NodesResponse(Nodes nodes)
    {
      this.nodes = nodes;
    }

    public Nodes getNodes()
    {
      return nodes;
    }

    public void setNodes(Nodes nodes)
    {
      this.nodes = nodes;
    }
  }

  public static class Nodes
  {
    // Argument-less constructor is needed for GSON parsing
    public Nodes()
    {
    }

    @SerializedName("node")
    private List<Node> nodeList;

    // Necessary for GSON parsing
    public Nodes(List<Node> nodeStats)
    {
      this.nodeList = nodeStats;
    }

    public List<Node> getNodeList()
    {
      return nodeList;
    }

    public void setNodeList(List<Node> nodeStats)
    {
      this.nodeList = nodeStats;
    }
  }

  public static class Node
  {
    /*
    /ws/v1/cluster/nodes REST endpoint fields:
    rack             string
    state            string
    id               string
    nodeHostName     string
    nodeHTTPAddress  string
    healthStatus     string
    healthReport     string
    lastHealthUpdate long
    usedMemoryMB     long
    availMemoryMB    long
    numContainers    int
    */

    String nodeHostName;
    String state;

    // Argument-less constructor is needed for GSON parsing
    public Node()
    {
    }

    // Necessary for GSON parsing
    public Node(String nodeHostName, String state)
    {
      this.nodeHostName = nodeHostName;
      this.state = state;
    }

    String getState()
    {
      return state;
    }

    String getNodeHostName()
    {
      return nodeHostName;
    }
  }
}
