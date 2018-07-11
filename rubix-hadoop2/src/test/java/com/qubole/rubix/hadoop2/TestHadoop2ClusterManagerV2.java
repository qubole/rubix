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

import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertTrue;

/**
 * Created by Abhishek on 7/3/18.
 */
@Test(singleThreaded = true)
public class TestHadoop2ClusterManagerV2
{
  private Log log = LogFactory.getLog(TestHadoop2ClusterManagerV2.class);
  private static TestWorker worker;
  private static Configuration conf;

  @BeforeClass
  public static void setupClass() throws IOException
  {
    worker = new TestWorker();
    conf = new Configuration();
    CacheConfig.setHadoopClusterManager(conf, "com.qubole.rubix.hadoop2.Hadoop2ClusterManagerV2");
    conf.set(Hadoop2ClusterManagerUtil.addressConf, TestHadoop2ClusterManagerUtil.HADOOP2_CLUSTER_ADDRESS);
  }

  @Test
  /*
   * Tests that the worker nodes returned are correctly handled by Hadoop2ClusterManager and sorted list of hosts is returned.
   */
  public void testGetNodes_multipleWorkers()
      throws IOException
  {
    final List<String> nodeHostnames = TestHadoop2ClusterManagerUtil.getNodeHostnamesFromCluster(
        TestHadoop2ClusterManagerUtil.CLUSTER_NODES_ENDPOINT,
        worker.new MultipleRunningWorkers(), conf, ClusterType.HADOOP2_CLUSTER_MANAGER);

    assertTrue(nodeHostnames.size() == 2, "Should only have two nodes");
    assertTrue(nodeHostnames.get(0).equals(TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_2) &&
        nodeHostnames.get(1).equals(TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_1), "Wrong nodes data");
  }

  @Test
  /*
   * Tests that the new worker nodes returned is correctly handled by Hadoop2ClusterManager and sorted list of hosts is returned.
   */
  public void testGetNodes_oneNewWorker()
      throws IOException
  {
    final List<String> nodeHostnames = TestHadoop2ClusterManagerUtil.getNodeHostnamesFromCluster(
        TestHadoop2ClusterManagerUtil.CLUSTER_NODES_ENDPOINT,
        worker.new MultipleWorkersOneNew(), conf, ClusterType.HADOOP2_CLUSTER_MANAGER);

    assertTrue(nodeHostnames.size() == 2, "Should only have two nodes");
    assertTrue(nodeHostnames.get(0).equals(TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_2) &&
        nodeHostnames.get(1).equals(TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_1), "Wrong nodes data");
  }

  @Test
  /*
   * Tests that in a cluster with decommissioned node, decommissioned node is not returned.
   */
  public void testUnhealthyNodeCluster_decommissioned()
      throws IOException
  {
    final List<String> nodeHostnames = TestHadoop2ClusterManagerUtil.getNodeHostnamesFromCluster(
        TestHadoop2ClusterManagerUtil.CLUSTER_NODES_ENDPOINT,
        worker.new MultipleWorkersOneDecommissioned(), conf, ClusterType.HADOOP2_CLUSTER_MANAGER);

    assertTrue(nodeHostnames.size() == 2, "Should have two nodes");
    assertTrue(nodeHostnames.get(0).equals(TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_2) &&
        nodeHostnames.get(1).equals(TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_1), "Wrong nodes data");
  }

  @Test
  /*
   * Tests that in a single node cluster, master node is returned as worker.
   */
  public void testMasterOnlyCluster()
      throws IOException
  {
    final List<String> nodeHostnames = TestHadoop2ClusterManagerUtil.getNodeHostnamesFromCluster(
        TestHadoop2ClusterManagerUtil.CLUSTER_NODES_ENDPOINT,
        worker.new NoWorkers(), conf, ClusterType.HADOOP2_CLUSTER_MANAGER);

    assertTrue(nodeHostnames.size() == 1, "Should have added localhost in list");
    assertTrue(nodeHostnames.get(0).equals(InetAddress.getLocalHost().getHostAddress()), "Not added right hostname");
  }

  @Test
  public void consistent_hashing_downScaling()
      throws IOException
  {
    int numKeys = 10;
    Set<String> keys = TestHadoop2ClusterManagerUtil.generateRandomKeys(numKeys);
    TestWorker sixLiveWorkers = worker.new SixWorkers();
    TestWorker fourLiveWorkerTwoDecommissioned = worker.new FourLiveWorkersTwoDecommissioned();

    int match = TestHadoop2ClusterManagerUtil.matchMemberships(sixLiveWorkers, fourLiveWorkerTwoDecommissioned, keys, conf, ClusterType.HADOOP2_CLUSTER_MANAGER);

    Map<String, Integer> keyMembership = TestHadoop2ClusterManagerUtil.getConsistentHashedMembership(sixLiveWorkers, keys, conf, ClusterType.HADOOP2_CLUSTER_MANAGER);

    int expected = 0;
    for (Map.Entry<String, Integer> entry : keyMembership.entrySet()) {
      if (entry.getValue() != 0 && entry.getValue() != 5) {
        expected++;
      }
    }

    assertTrue(match == expected, "Distribution of the keys didn't match");
  }

  @Test
  public void consistent_hashing_spotloss()
      throws IOException
  {
    String key = "1";
    final List<String> nodeHostnames1 = TestHadoop2ClusterManagerUtil.getNodeHostnamesFromCluster(
        TestHadoop2ClusterManagerUtil.CLUSTER_NODES_ENDPOINT, worker.new FourWorkers(),
        conf, ClusterType.HADOOP2_CLUSTER_MANAGER);

    final List<String> nodeHostnames2 = TestHadoop2ClusterManagerUtil.getNodeHostnamesFromCluster(
        TestHadoop2ClusterManagerUtil.CLUSTER_NODES_ENDPOINT, worker.new FourLiveWorkersOneDecommissioned(),
        conf, ClusterType.HADOOP2_CLUSTER_MANAGER);

    assertTrue(nodeHostnames1.size() == 4, "Should have four nodes");
    assertTrue(nodeHostnames2.size() == 5, "Should have five nodes");

    int nodeIndex1 = TestHadoop2ClusterManagerUtil.getConsistentHashedNodeIndexFromCluster(
        TestHadoop2ClusterManagerUtil.CLUSTER_NODES_ENDPOINT, worker.new FourWorkers(), key,
        conf, ClusterType.HADOOP2_CLUSTER_MANAGER);

    int nodeIndex2 = TestHadoop2ClusterManagerUtil.getConsistentHashedNodeIndexFromCluster(
        TestHadoop2ClusterManagerUtil.CLUSTER_NODES_ENDPOINT, worker.new FourLiveWorkersOneDecommissioned(), key,
        conf, ClusterType.HADOOP2_CLUSTER_MANAGER);

    String nodeName1 = nodeHostnames1.get(nodeIndex1);
    String nodeName2 = nodeHostnames2.get(nodeIndex2);

    assertTrue(nodeName1.equals(nodeName2), "Both should be the same node");
  }
}
