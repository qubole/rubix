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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.qubole.rubix.core.ClusterManager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by sakshia on 28/7/16.
 */
public class Hadoop2ClusterManager
        extends ClusterManager
{
    private boolean isMaster = true;
    public int serverPort = 8088;
    private String serverAddress = "localhost";
    private Supplier<List<String>> nodesSupplier;
    String address = "localhost:8088";
    private Log log = LogFactory.getLog(Hadoop2ClusterManager.class);
    static String addressConf = "yarn.resourcemanager.webapp.address";

    @Override
    public void initialize(Configuration conf)
    {
        super.initialize(conf);
        log.debug("Initializing: Hadoop2ClusterManager");
        this.address = conf.get(addressConf, address);
        this.serverAddress = address.substring(0, address.indexOf(":"));
        this.serverPort = Integer.parseInt(address.substring(address.indexOf(":") + 1));
        nodesSupplier = Suppliers.memoizeWithExpiration(new Supplier<List<String>>()
        {
            @Override
            public List<String> get()
            {
                if (!isMaster) {
                    // First time all nodes start assuming themselves as master and down the line figure out their role
                    // Next time onwards, only master will be fetching the list of nodes
                    return ImmutableList.of();
                }
                try {
                    StringBuffer response = new StringBuffer();
                    URL obj = getNodeURL();
                    HttpURLConnection httpcon = (HttpURLConnection) obj.openConnection();
                    httpcon.setRequestMethod("GET");
                    int responseCode = httpcon.getResponseCode();
                    log.debug("Sending 'GET' request to URL: " + obj.toString());
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
                        isMaster = false;
                        httpcon.disconnect();
                        return ImmutableList.of();
                    }
                    Gson gson = new Gson();
                    Type type = new TypeToken<Nodes>() {}.getType();
                    Nodes nodes = gson.fromJson(response.toString(), type);
                    List<Elements> allNodes = nodes.getNodes().getNode();
                    Set<String> hosts = new HashSet<>();

                    for (Elements node : allNodes) {
                        String state = node.getState();
                        log.debug("Hostname: " + node.getNodeHostName() + "State: " + state);
                        //keep only healthy data nodes
                        if (state.equalsIgnoreCase("Running") || state.equalsIgnoreCase("New") || state.equalsIgnoreCase("Rebooted")) {
                            hosts.add(node.getNodeHostName());
                        }
                    }

                    if (hosts.isEmpty()) {
                        throw new Exception("No healthy data nodes found.");
                    }

                    List<String> hostList = Lists.newArrayList(hosts.toArray(new String[0]));
                    Collections.sort(hostList);
                    log.debug("Hostlist: " + hostList.toString());
                    return hostList;
                }
                catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        }, 10, TimeUnit.SECONDS);
    }

    public URL getNodeURL()
            throws MalformedURLException
    {
        return new URL("http://" + serverAddress + ":" + serverPort + "/ws/v1/cluster/nodes");
    }

    @Override
    public boolean isMaster()
    {
        // issue get on nodesSupplier to ensure that isMaster is set correctly
        nodesSupplier.get();
        return isMaster;
    }

    @Override
    public List<String> getNodes()
    {
        return nodesSupplier.get();
    }

    public static class Nodes
    {
        public Nodes()
        {
        }
        private Node nodes;

        public void setNodes(Node nodes)
        {
            this.nodes = nodes;
        }
        public Node getNodes()
        {
            return nodes;
        }
    }

    public static class Node
    {
        public Node()
        {
        }
        private List<Elements> node;

        public void setNode(List<Elements> node)
        {
            this.node = node;
        }
        public List<Elements> getNode()
        {
            return node;
        }
    }

    public static class Elements
    {
        /*
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
