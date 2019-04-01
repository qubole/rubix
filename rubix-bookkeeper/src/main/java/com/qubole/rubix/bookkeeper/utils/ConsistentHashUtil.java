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

package com.qubole.rubix.bookkeeper.utils;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.qubole.rubix.spi.thrift.NodeState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ConsistentHashUtil
{
  private static Log log = LogFactory.getLog(ConsistentHashUtil.class.getName());

  private ConsistentHashUtil()
  {
    //
  }

  public static String getHashedNodeForKey(Map<String, NodeState> nodesMap, String key)
  {
    List<String> nodes = Lists.newArrayList(nodesMap.keySet().toArray(new String[0]));
    int nodeIndex = getNodeIndex(nodesMap, key);
    return nodes.get(nodeIndex);
  }

  public static int getNodeIndex(Map<String, NodeState> nodesMap, String key)
  {
    HashFunction hf = Hashing.md5();
    HashCode hc = hf.hashString(key, Charsets.UTF_8);

    int nodeIndex = Hashing.consistentHash(hc, nodesMap.size());
    if (hc.asInt() % 2 == 0) {
      nodeIndex = getNextRunningNodeIndex(nodesMap, nodeIndex);
    }
    else {
      nodeIndex = getPreviousRunningNodeIndex(nodesMap, nodeIndex);
    }

    return nodeIndex;
  }

  private static Integer getNextRunningNodeIndex(Map<String, NodeState> nodesMap, int startIndex)
  {
    List<String> nodeList = new ArrayList<>(nodesMap.keySet());
    for (int i = startIndex; i < (startIndex + nodeList.size()); i++) {
      int index = i >= nodeList.size() ? (i - nodeList.size()) : i;
      NodeState nodeState = nodesMap.get(nodeList.get(index));
      if (nodeState == NodeState.ACTIVE) {
        return index;
      }
    }

    return null;
  }

  private static Integer getPreviousRunningNodeIndex(Map<String, NodeState> nodesMap, int startIndex)
  {
    List<String> nodeList = new ArrayList<>(nodesMap.keySet());
    for (int i = startIndex; i >= 0; i--) {
      NodeState nodeState = nodesMap.get(nodeList.get(i));
      if (nodeState == NodeState.ACTIVE) {
        return i;
      }
    }

    for (int i = nodeList.size() - 1; i > startIndex; i--) {
      NodeState nodeState = nodesMap.get(nodeList.get(i));
      if (nodeState == NodeState.ACTIVE) {
        return i;
      }
    }

    return null;
  }
}
