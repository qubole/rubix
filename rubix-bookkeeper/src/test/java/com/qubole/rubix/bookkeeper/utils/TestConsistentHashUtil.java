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

import com.qubole.rubix.spi.thrift.NodeState;
import org.testng.annotations.Test;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertTrue;

public class TestConsistentHashUtil
{
  @Test
  public void consistent_hashing_downScaling()
  {
    int numKeys = 10;
    Set<String> keys = generateRandomKeys(numKeys);
    Map<String, NodeState> sixLiveWorkers = getNodes(6, 0);
    Map<String, NodeState> fourLiveWorkerTwoDecommissioned = getNodes(4, 0);

    int match = matchMemberships(sixLiveWorkers, fourLiveWorkerTwoDecommissioned, keys);

    Map<String, String> keyMembership = getConsistentHashedMembership(sixLiveWorkers, keys);

    int expected = 0;
    for (Map.Entry<String, String> entry : keyMembership.entrySet()) {
      if (fourLiveWorkerTwoDecommissioned.get(entry.getValue()) == NodeState.ACTIVE) {
        expected++;
      }
    }

    assertTrue(match == expected, "Distribution of the keys didn't match");
  }

  @Test
  public void consistent_hashing_spotloss()
  {
    String key = "1";

    Map<String, NodeState> fourLiveWorkers = getNodes(4, 0);
    Map<String, NodeState> fourLiveWorkerOneDecommissioned = getNodes(4, 1);

    String nodeHost1 = getConsistentHashedNodeIndex(fourLiveWorkers, key);

    String nodeHost2 = getConsistentHashedNodeIndex(fourLiveWorkerOneDecommissioned, key);

    assertTrue(nodeHost1.equals(nodeHost2), "Both should be the same node");
  }

  // Utils Methods

  static Set<String> generateRandomKeys(int numKeys)
  {
    Set<String> keys = new HashSet<>();
    for (int i = 0; i < numKeys; i++) {
      keys.add(getSaltString());
    }

    return keys;
  }

  static String getSaltString()
  {
    String saltchars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    StringBuilder salt = new StringBuilder();
    SecureRandom rnd = new SecureRandom();
    while (salt.length() < 18) { // length of the random string.
      int index = (int) (rnd.nextFloat() * saltchars.length());
      salt.append(saltchars.charAt(index));
    }
    String saltStr = salt.toString();
    return saltStr;
  }

  static int matchMemberships(Map<String, NodeState> prevWorker, Map<String, NodeState> newWorker, Set<String> keys)
  {
    Map<String, String> keyMembership1 = getConsistentHashedMembership(prevWorker, keys);
    Map<String, String> keyMembership2 = getConsistentHashedMembership(newWorker, keys);

    int match = 0;
    int nonMatch = 0;

    for (String key : keys) {
      if (keyMembership1.get(key).equals(keyMembership2.get(key))) {
        match++;
      }
      else {
        nonMatch++;
      }
    }

    return match;
  }

  static Map<String, String> getConsistentHashedMembership(Map<String, NodeState> nodesMap, Set<String> keys)
  {
    Map<String, String> keyMembership = new HashMap<>();
    String host;

    for (String key : keys) {
      host = ConsistentHashUtil.getHashedNodeForKey(nodesMap, key);
      keyMembership.put(key, host);
    }
    return keyMembership;
  }

  static Map<String, NodeState> getNodes(int liveWorkers, int inactiveWorkers)
  {
    Map<String, NodeState> map = new HashMap<>();
    for (int i = 0; i < liveWorkers; i++) {
      map.put(Integer.toString(i), NodeState.ACTIVE);
    }

    for (int i = liveWorkers; i < (liveWorkers + inactiveWorkers); i++) {
      map.put(Integer.toString(i), NodeState.INACTIVE);
    }

    return map;
  }

  static String getConsistentHashedNodeIndex(Map<String, NodeState> nodesMap, String key)
  {
    final String host = ConsistentHashUtil.getHashedNodeForKey(nodesMap, key);
    return host;
  }
}
