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
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestHadoop2ClusterManager
{
  private Log log = LogFactory.getLog(TestHadoop2ClusterManager.class);
  private static TestWorker worker;
  private static Configuration conf;

  @BeforeClass
  public static void setupClass() throws IOException
  {
    worker = new TestWorker();
    conf = new Configuration();
    CacheConfig.setHadoopClusterManager(conf, "com.qubole.rubix.hadoop2.Hadoop2ClusterManager");
    conf.set(Hadoop2ClusterManagerUtil.addressConf, TestHadoop2ClusterManagerUtil.HADOOP2_CLUSTER_ADDRESS);
  }

  /**
   * Initializes a {@link Hadoop2ClusterManager} for testing.
   *
   * @return The cluster manager instance to be tested.
   */
  static ClusterManager buildClusterManager()
  {
    final ClusterManager clusterManager = new Hadoop2ClusterManager();
    final Configuration conf = new Configuration();
    conf.set(Hadoop2ClusterManagerUtil.addressConf, TestHadoop2ClusterManagerUtil.HADOOP2_CLUSTER_ADDRESS);
    clusterManager.initialize(conf);
    return clusterManager;
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
    assertTrue(nodeHostnames.get(0).equals(TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_1) &&
        nodeHostnames.get(1).equals(TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_2), "Wrong nodes data");
  }

  @Test
  /*
   * Tests that the single worker node returned is correctly handled by Hadoop2ClusterManager.
   */
  public void testGetNodes_oneWorker()
      throws IOException
  {
    final List<String> nodeHostnames = TestHadoop2ClusterManagerUtil.getNodeHostnamesFromCluster(
        TestHadoop2ClusterManagerUtil.CLUSTER_NODES_ENDPOINT,
        worker.new OneRunningWorker(), conf, ClusterType.HADOOP2_CLUSTER_MANAGER);

    assertTrue(nodeHostnames.size() == 1, "Should only have one node");
    assertTrue(nodeHostnames.get(0).equals(TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_1));
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
    assertTrue(nodeHostnames.get(0).equals(TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_1) &&
        nodeHostnames.get(1).equals(TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_2), "Wrong nodes data");
  }

  @Test
  /*
   * Tests that the rebooted worker node returned is correctly handled by Hadoop2ClusterManager and sorted list of hosts is returned.
   */
  public void testGetNodes_oneRebootedWorker()
      throws IOException
  {
    final List<String> nodeHostnames = TestHadoop2ClusterManagerUtil.getNodeHostnamesFromCluster(
        TestHadoop2ClusterManagerUtil.CLUSTER_NODES_ENDPOINT,
        worker.new MultipleWorkersOneRebooted(), conf, ClusterType.HADOOP2_CLUSTER_MANAGER);

    assertTrue(nodeHostnames.size() == 2, "Should only have two nodes");
    assertTrue(nodeHostnames.get(0).equals(TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_1) &&
        nodeHostnames.get(1).equals(TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_2), "Wrong nodes data");
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
  /*
   * Tests that in a cluster with decommissioned node, decommissioned node is not returned.
   */
  public void testUnhealthyNodeCluster_decommissioned()
      throws IOException
  {
    final List<String> nodeHostnames = TestHadoop2ClusterManagerUtil.getNodeHostnamesFromCluster(
        TestHadoop2ClusterManagerUtil.CLUSTER_NODES_ENDPOINT,
        worker.new MultipleWorkersOneDecommissioned(), conf, ClusterType.HADOOP2_CLUSTER_MANAGER);

    assertTrue(nodeHostnames.size() == 1, "Should only have one node");
    assertTrue(nodeHostnames.get(0).equals(TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_2), "Wrong nodes data");
  }

  @Test
  /*
   * Tests that in a cluster with decommissioning node, decommissioning node is not returned.
   */
  public void testUnhealthyNodeCluster_decommissioning()
      throws IOException
  {
    final List<String> nodeHostnames = TestHadoop2ClusterManagerUtil.getNodeHostnamesFromCluster(
        TestHadoop2ClusterManagerUtil.CLUSTER_NODES_ENDPOINT,
        worker.new MultipleWorkersOneDecommissioning(), conf, ClusterType.HADOOP2_CLUSTER_MANAGER);

    assertTrue(nodeHostnames.size() == 1, "Should only have one node");
    assertTrue(nodeHostnames.get(0).equals(TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_2), "Wrong nodes data");
  }

  @Test
  /*
   * Tests that in a cluster with lost node, lost node is not returned.
   */
  public void testUnhealthyNodeCluster_lost()
      throws IOException
  {
    final List<String> nodeHostnames = TestHadoop2ClusterManagerUtil.getNodeHostnamesFromCluster(
        TestHadoop2ClusterManagerUtil.CLUSTER_NODES_ENDPOINT,
        worker.new MultipleWorkersOneLost(), conf, ClusterType.HADOOP2_CLUSTER_MANAGER);

    assertTrue(nodeHostnames.size() == 1, "Should only have one node");
    assertTrue(nodeHostnames.get(0).equals(TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_2), "Wrong nodes data");
  }

  @Test
  /*
   * Tests that in a cluster with unhealthy node, unhealthy node is not returned.
   */
  public void testUnhealthyNodeCluster_unhealthy()
      throws IOException
  {
    final List<String> nodeHostnames = TestHadoop2ClusterManagerUtil.getNodeHostnamesFromCluster(
        TestHadoop2ClusterManagerUtil.CLUSTER_NODES_ENDPOINT,
        worker.new MultipleWorkersOneUnhealthy(), conf, ClusterType.HADOOP2_CLUSTER_MANAGER);

    assertTrue(nodeHostnames.size() == 1, "Should only have one node");
    assertTrue(nodeHostnames.get(0).equals(TestHadoop2ClusterManagerUtil.WORKER_HOSTNAME_2), "Wrong nodes data");
  }

  @Test
  public void testClusterIndex()
      throws IOException
  {
    final List<String> nodeHostnames = TestHadoop2ClusterManagerUtil.getNodeHostnamesFromCluster(
        TestHadoop2ClusterManagerUtil.CLUSTER_NODES_ENDPOINT,
        worker.new MultipleWorkersOneNew(), conf, ClusterType.HADOOP2_CLUSTER_MANAGER);
    ClusterManager manager = TestHadoop2ClusterManagerUtil.getClusterManagerInstance(ClusterType.HADOOP2_CLUSTER_MANAGER, conf);
    int index = manager.getNodeIndex(nodeHostnames.size(), "1");

    assertTrue(index == 1, "Consistent Hasing logic returned wrong node index");
  }
}
