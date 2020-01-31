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

package com.qubole.rubix.common.utils;

import com.qubole.rubix.spi.CacheConfig;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestClusterUtil
{
  private static final String TEST_DEFAULT_MASTER_HOSTNAME = "localhost";
  private static final String TEST_MASTER_HOSTNAME = "123.456.789.0";
  private static final String TEST_YARN_RESOURCEMANAGER_ADDRESS = "255.255.255.1:1234";
  private static final String TEST_YARN_RESOURCEMANAGER_HOSTNAME = "255.255.255.1";

  private final Configuration conf = new Configuration();

  @AfterMethod
  public void clearConfiguration()
  {
    conf.clear();
  }

  /**
   * Verify that the <code>master.hostname</code> configuration value is returned if it is available.
   */
  @Test
  public void testGetMasterHostname_masterHostnameConf()
  {
    CacheConfig.setCoordinatorHostName(conf, TEST_MASTER_HOSTNAME);
    CacheConfig.setResourceManagerAddress(conf, TEST_YARN_RESOURCEMANAGER_ADDRESS);

    final String hostname = ClusterUtil.getMasterHostname(conf);
    assertEquals(hostname, TEST_MASTER_HOSTNAME, "Unexpected hostname!");
  }

  /**
   * Verify that the <code>yarn.resourcemanager.address</code> configuration value is returned if <code>master.hostname</code> is not available.
   */
  @Test
  public void testGetMasterHostname_yarnResourceManagerConf()
  {
    CacheConfig.setResourceManagerAddress(conf, TEST_YARN_RESOURCEMANAGER_ADDRESS);

    final String hostname = ClusterUtil.getMasterHostname(conf);
    assertEquals(hostname, TEST_YARN_RESOURCEMANAGER_HOSTNAME, "Unexpected hostname!");
  }

  /**
   * Verify that the default hostname is returned if alternative options are not available.
   */
  @Test
  public void testGetMasterHostname_noConfFound()
  {
    final String hostname = ClusterUtil.getMasterHostname(conf);
    assertEquals(hostname, TEST_DEFAULT_MASTER_HOSTNAME, "Unexpected hostname!");
  }
}
