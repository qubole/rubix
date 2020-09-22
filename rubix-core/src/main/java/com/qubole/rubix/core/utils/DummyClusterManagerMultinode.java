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

import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class DummyClusterManagerMultinode extends ClusterManager
{
    @Override
    public List<String> getNodesInternal()
    {
        List<String> list = new ArrayList<String>();
        String hostName = "";
        try {
            hostName = InetAddress.getLocalHost().getCanonicalHostName();
        }
        catch (UnknownHostException e) {
            hostName = "localhost";
        }

        list.add(hostName);
        list.add(hostName + "_copy");

        return list;
    }

    @Override
    public ClusterType getClusterType()
    {
        return ClusterType.TEST_CLUSTER_MANAGER;
    }

    @Override
    public String getCurrentNodeName()
    {
        return getNodes().get(0);
    }

    public String locateKey(String key)
    {
        return getNodes().get(1);
    }
}
