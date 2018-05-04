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
package com.qubole.rubix.bookkeeper;

import com.codahale.metrics.MetricRegistry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestBookKeeperServer
{
  private static final Log log = LogFactory.getLog(TestBookKeeperServer.class.getName());

  private MetricRegistry metrics;
  private Configuration conf;

  @BeforeMethod
  public void setUp()
  {
    conf = new Configuration();
    metrics = new MetricRegistry();
  }

  /**
   * Verify that liveness status of the BookKeeper daemon is correctly reported.
   *
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   */
  @Test
  public void verifyLivenessCheck() throws InterruptedException
  {
    assertNull(metrics.getGauges().get(BookKeeperServer.METRIC_BOOKKEEPER_LIVENESS_CHECK), "Metric should not exist before server has started");

    startBookKeeperServer();

    assertEquals(metrics.getGauges().get(BookKeeperServer.METRIC_BOOKKEEPER_LIVENESS_CHECK).getValue(), 1, "Metric should return a value once the server has started");

    stopBookKeeperServer();

    assertNull(metrics.getGauges().get(BookKeeperServer.METRIC_BOOKKEEPER_LIVENESS_CHECK), "Metric should not exist after server has stopped");
  }

  /**
   * Start an instance of the BookKeeper server.
   *
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   */
  private void startBookKeeperServer() throws InterruptedException
  {
    final Thread thread = new Thread()
    {
      public void run()
      {
        BookKeeperServer.startServer(conf, metrics);
      }
    };
    thread.start();

    while (!BookKeeperServer.isServerUp()) {
      Thread.sleep(200);
      log.info("Waiting for BookKeeper Server to come up");
    }
  }

  /**
   * Stop the currently running BookKeeper server instance.
   *
   * @throws InterruptedException if the current thread is interrupted while sleeping.
   */
  private void stopBookKeeperServer() throws InterruptedException
  {
    BookKeeperServer.stopServer();
    while (BookKeeperServer.isServerUp()) {
      Thread.sleep(200);
      log.info("Waiting for BookKeeper Server to shut down");
    }
  }
}
