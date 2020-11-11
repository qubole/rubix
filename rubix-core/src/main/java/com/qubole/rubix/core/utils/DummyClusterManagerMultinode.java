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
package com.qubole.rubix.core.utils;

import com.qubole.rubix.spi.AsyncClusterManager;
import com.qubole.rubix.spi.ClusterType;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DummyClusterManagerMultinode extends AsyncClusterManager
{
    private final String currentNode;
    private final String otherNode;
    private final Set<String> nodes = new HashSet<>();

    public DummyClusterManagerMultinode()
    {
        String currentNode;
        try {
            currentNode = InetAddress.getLocalHost().getCanonicalHostName();
        }
        catch (UnknownHostException e) {
            currentNode = "localhost";
        }
        this.currentNode = currentNode;
        nodes.add(currentNode);
        this.otherNode = currentNode + "_copy";
        nodes.add(otherNode);
    }

    @Override
    public Set<String> getNodesInternal()
    {
        return nodes;
    }

    @Override
    public ClusterType getClusterType()
    {
        return ClusterType.TEST_CLUSTER_MANAGER;
    }

    @Override
    public String getCurrentNodeName()
    {
        return currentNode;
    }

    public String locateKey(String key)
    {
        return otherNode;
    }
}
