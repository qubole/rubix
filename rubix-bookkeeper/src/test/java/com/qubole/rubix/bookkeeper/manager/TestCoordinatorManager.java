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

package com.qubole.rubix.bookkeeper.manager;

import com.codahale.metrics.MetricRegistry;
import com.qubole.rubix.spi.CacheConfig;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestCoordinatorManager
{
  private static final String WORKER1_HOSTNAME = "worker1";
  private static final String WORKER2_HOSTNAME = "worker2";

  private Configuration conf;
  private MetricRegistry metrics;

  @BeforeMethod
  public void setUp()
  {
    this.conf = new Configuration();
    this.metrics = new MetricRegistry();
  }

  /**
   * Verify that the worker liveness count metric is correctly registered.
   */
  @Test
  public void testWorkerLivenessCountMetric()
  {
    final CoordinatorManager coordinatorManager = new CoordinatorManager(conf, metrics);
    coordinatorManager.handleHeartbeat(WORKER1_HOSTNAME);
    coordinatorManager.handleHeartbeat(WORKER2_HOSTNAME);

    int workerCount = (int) metrics.getGauges().get(CoordinatorManager.METRIC_BOOKKEEPER_LIVE_WORKER_GAUGE).getValue();
    assertEquals(workerCount, 2, "Incorrect number of workers reporting heartbeat");
  }

  /**
   * Verify that the worker liveness status properly expires.
   *
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   */
  @Test
  public void testWorkerLivenessCountMetric_workerLivenessExpired() throws InterruptedException
  {
    final int workerLivenessExpiry = 5000; // ms
    CacheConfig.setWorkerLivenessExpiry(conf, workerLivenessExpiry);

    final CoordinatorManager coordinatorManager = new CoordinatorManager(conf, metrics);
    coordinatorManager.handleHeartbeat(WORKER1_HOSTNAME);
    coordinatorManager.handleHeartbeat(WORKER2_HOSTNAME);

    int workerCount = (int) metrics.getGauges().get(CoordinatorManager.METRIC_BOOKKEEPER_LIVE_WORKER_GAUGE).getValue();
    assertEquals(workerCount, 2, "Incorrect number of workers reporting heartbeat");

    Thread.sleep(workerLivenessExpiry);
    coordinatorManager.handleHeartbeat(WORKER1_HOSTNAME);

    workerCount = (int) metrics.getGauges().get(CoordinatorManager.METRIC_BOOKKEEPER_LIVE_WORKER_GAUGE).getValue();
    assertEquals(workerCount, 1, "Incorrect number of workers reporting heartbeat");
  }
}
