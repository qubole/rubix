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
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.qubole.rubix.spi.thrift.ClusterNode;
import com.qubole.rubix.spi.thrift.NodeState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

public class ConsistentHashUtil
{
  private static Log log = LogFactory.getLog(ConsistentHashUtil.class.getName());

  private ConsistentHashUtil()
  {
    //
  }

  public static String getHashedNodeForKey(List<ClusterNode> nodeList, String key)
  {
    int nodeIndex = getNodeIndex(nodeList, key);
    return nodeList.get(nodeIndex).nodeUrl;
  }

  public static int getNodeIndex(List<ClusterNode> nodeList, String key)
  {
    HashFunction hf = Hashing.md5();
    HashCode hc = hf.hashString(key, Charsets.UTF_8);

    int nodeIndex = Hashing.consistentHash(hc, nodeList.size());
    if (hc.asInt() % 2 == 0) {
      nodeIndex = getNextRunningNodeIndex(nodeList, nodeIndex);
    }
    else {
      nodeIndex = getPreviousRunningNodeIndex(nodeList, nodeIndex);
    }

    return nodeIndex;
  }

  private static Integer getNextRunningNodeIndex(List<ClusterNode> nodeList, int startIndex)
  {
    for (int i = startIndex; i < (startIndex + nodeList.size()); i++) {
      int index = i >= nodeList.size() ? (i - nodeList.size()) : i;
      NodeState nodeState = nodeList.get(index).nodeState;
      if (nodeState == NodeState.ACTIVE) {
        return index;
      }
    }

    return null;
  }

  private static Integer getPreviousRunningNodeIndex(List<ClusterNode> nodeList, int startIndex)
  {
    for (int i = startIndex; i >= 0; i--) {
      NodeState nodeState = nodeList.get(i).nodeState;
      if (nodeState == NodeState.ACTIVE) {
        return i;
      }
    }

    for (int i = nodeList.size() - 1; i > startIndex; i--) {
      NodeState nodeState = nodeList.get(i).nodeState;
      if (nodeState == NodeState.ACTIVE) {
        return i;
      }
    }

    return null;
  }
}
