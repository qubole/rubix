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
package com.qubole.rubix.bookkeeper;

import com.codahale.metrics.MetricRegistry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.management.MalformedObjectNameException;

public class TestLocalDataTransferServer extends BaseServerTest
{
  private static final Log log = LogFactory.getLog(TestLocalDataTransferServer.class);

  private final Configuration conf = new Configuration();
  private MetricRegistry metrics;

  @BeforeMethod
  public void setUp()
  {
    metrics = new MetricRegistry();
  }

  @AfterMethod
  public void tearDown()
  {
    conf.clear();
  }

  /**
   * Verify that JVM metrics are registered when configured to.
   *
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   */
  @Test
  public void testJvmMetricsEnabled() throws InterruptedException, MalformedObjectNameException
  {
    super.testJvmMetrics(ServerType.LOCAL_DATA_TRANSFER_SERVER, conf, metrics, true);
  }

  /**
   * Verify that JVM metrics are not registered when configured not to.
   *
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   */
  @Test
  public void testJvmMetricsNotEnabled() throws InterruptedException, MalformedObjectNameException
  {
    super.testJvmMetrics(ServerType.LOCAL_DATA_TRANSFER_SERVER, conf, metrics, false);
  }

  /**
   * Verify that all registered metrics are removed once the Local Data Transfer Server has stopped.
   */
  @Test
  public void verifyMetricsAreRemoved() throws InterruptedException
  {
    super.verifyMetricsAreRemoved(ServerType.LOCAL_DATA_TRANSFER_SERVER, conf, metrics);
  }
}
