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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.json.JsonCodec.listJsonCodec;

/**
 * Created by stagra on 14/1/16.
 */
public class PrestoClusterManager extends ClusterManager
{
    private boolean isMaster = true;
    private int serverPort = 8081;
    private String serverAddress = "http://localhost";

    private Supplier<List<String>> nodesSupplier;

    private static final Logger log = Logger.get(PrestoClusterManager.class);

    public static String serverPortConf = "caching.fs.presto-server-port";

    // Safe to use single instance of HttpClient since Supplier.get() provides synchronization
    private static HttpClient httpClient = new JettyHttpClient();

    @Override
    public void initialize(Configuration conf)
    {
        super.initialize(conf);
        this.serverPort = conf.getInt(serverPortConf, serverPort);

        nodesSupplier = Suppliers.memoizeWithExpiration(new Supplier<List<String>>() {
            @Override
            public List<String> get()
            {
                if (!isMaster) {
                    // First time all nodes start assuming themselves as master and down the line figure out their role
                    // Next time onwards, only master will be fetching the list of nodes
                    return ImmutableList.of();
                }

                try {
                    List<Stats> allNodes;
                    List<Stats> failedNodes = ImmutableList.of();

                    Request allNodesRequest = prepareGet()
                            .setUri(getNodeUri())
                            .build();

                    Request failedNodesRequest = prepareGet()
                            .setUri(getFailedNodeUri())
                            .build();

                    Future allNodesFuture = httpClient.executeAsync(allNodesRequest, createFullJsonResponseHandler(listJsonCodec(Stats.class)));
                    Future failedNodesFuture = httpClient.executeAsync(failedNodesRequest, createFullJsonResponseHandler(listJsonCodec(Stats.class)));

                    FullJsonResponseHandler.JsonResponse allNodesResponse = (FullJsonResponseHandler.JsonResponse) allNodesFuture.get();
                    if (allNodesResponse.getStatusCode() == HttpStatus.OK.code()) {
                        isMaster = true;
                        if (!allNodesResponse.hasValue()) {
                            // Empty result set => server up and only master node running, return localhost has the only node
                            // Do not need to consider failed nodes list as 1node cluster and server is up since it replied to allNodesRequest
                            return ImmutableList.of(InetAddress.getLocalHost().getHostName());
                        }
                        else {
                            allNodes = (List<Stats>) allNodesResponse.getValue();
                        }
                    }
                    else {
                        log.info(String.format("v1/node failed with code: %d setting this node as worker ", allNodesResponse.getStatusCode()));
                        isMaster = false;
                        return ImmutableList.of();
                    }

                    // check on failed nodes
                    FullJsonResponseHandler.JsonResponse failedNodesResponse = (FullJsonResponseHandler.JsonResponse) failedNodesFuture.get();
                    if (failedNodesResponse.getStatusCode() == HttpStatus.OK.code()) {
                        if (!failedNodesResponse.hasValue()) {
                            failedNodes = ImmutableList.of();
                            //return ImmutableList.of(InetAddress.getLocalHost().getHostName());
                        }
                        else {
                            failedNodes = (List<Stats>) failedNodesResponse.getValue();
                        }
                    }

                    // keep only the healthy nodes
                    allNodes.removeAll(failedNodes);

                    Set<String> hosts = new HashSet<String>();

                    for (Stats node : allNodes) {
                        hosts.add(node.getUri().getHost());
                        log.debug(String.format("Node: %s", node.getUri()));
                    }

                    if (hosts.isEmpty()) {
                        // case of master only cluster
                        hosts.add(InetAddress.getLocalHost().getHostName());
                    }

                    List<String> hostList = Lists.newArrayList(hosts.toArray(new String[0]));
                    Collections.sort(hostList);
                    return hostList;
                }
                catch (InterruptedException | ExecutionException | URISyntaxException | UnknownHostException e) {
                    throw Throwables.propagate(e);
                }
            }
        }, 10, TimeUnit.SECONDS);
    }

    @Override
    public boolean isMaster()
    {
        // issue get on nodesSupplier to ensure that isMaster is set correctly
        nodesSupplier.get();
        return isMaster;
    }

    /*
     * This returns list of worker nodes when there are worker nodes in the cluster
     * If it is a single node cluster, it will return localhost information
     */
    @Override
    public List<String> getNodes()
    {
        return nodesSupplier.get();
    }

    @Override
    public ClusterType getClusterType()
    {
        return ClusterType.PRESTO_CLUSTER_MANAGER;
    }

    private URI getNodeUri()
            throws URISyntaxException
    {
        return new URI(serverAddress + ":" + serverPort + "/v1/node");
    }

    private URI getFailedNodeUri()
            throws URISyntaxException
    {
        return new URI(serverAddress + ":" + serverPort + "/v1/node/failed");
    }

    public static class Stats
    {
        URI uri;
        String lastResponseTime;

        @JsonCreator
        public Stats(@JsonProperty("uri") URI uri,
                @JsonProperty("lastResponseTime") String lastResponseTime)
        {
            this.uri = uri;
            this.lastResponseTime = lastResponseTime;
        }

        @JsonProperty
        public URI getUri()
        {
            return uri;
        }

        public void setURI(URI uri)
        {
            this.uri = uri;
        }

        @JsonProperty String getLastResponseTime()
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
            return uri.equals(o.getUri()) && lastResponseTime.equals(o.getLastResponseTime());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(uri, lastResponseTime);
        }
    }
}
