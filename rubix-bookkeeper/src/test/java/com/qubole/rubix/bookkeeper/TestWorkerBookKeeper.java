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

package com.qubole.rubix.bookkeeper;

import com.codahale.metrics.MetricRegistry;
import com.qubole.rubix.bookkeeper.test.BookKeeperTestUtils;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.transport.TSocket;
import org.apache.thrift.shaded.transport.TTransportException;
import org.mockito.ArgumentMatchers;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestWorkerBookKeeper
{
  private static final Log log = LogFactory.getLog(TestWorkerBookKeeper.class);

  private static final String TEST_CACHE_DIR_PREFIX = BookKeeperTestUtils.getTestCacheDirPrefix("TestWorkerBookKeeper");
  private static final int TEST_MAX_DISKS = 1;
  private static final int TEST_MAX_RETRIES = 5;
  private static final int TEST_RETRY_INTERVAL = 500;

  private final Configuration conf = new Configuration();

  @BeforeClass
  public void setUpForClass() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);

    BookKeeperTestUtils.createCacheParentDirectories(conf, TEST_MAX_DISKS);
  }

  @BeforeMethod
  public void setUp() throws InterruptedException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);
    CacheConfig.setServiceRetryInterval(conf, TEST_RETRY_INTERVAL);
    CacheConfig.setServiceMaxRetries(conf, TEST_MAX_RETRIES);

    CacheConfig.setOnMaster(conf, true);
    startBookKeeperServer();
  }

  @AfterMethod
  public void tearDown()
  {
    conf.clear();

    stopBookKeeperServer();
  }

  @AfterClass
  public void tearDownForClass() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);

    BookKeeperTestUtils.removeCacheParentDirectories(conf, TEST_MAX_DISKS);
  }

  /**
   * Verify that WorkerBookKeeper throws the correct exception when asked to handle heartbeats.
   *
   * @throws FileNotFoundException if the parent directory for the cache cannot be found when initializing the BookKeeper.
   */
  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testHandleHeartbeat_shouldNotBeHandled() throws FileNotFoundException
  {
    final WorkerBookKeeper workerBookKeeper = new WorkerBookKeeper(conf, new MetricRegistry());
    workerBookKeeper.handleHeartbeat("");
  }

  /**
   * Verify that the heartbeat service correctly makes a connection using a BookKeeper client.
   *
   * @throws TTransportException if the BookKeeper client cannot be created.
   */
  @Test
  public void testHeartbeatRetryLogic_noRetriesNeeded() throws TTransportException
  {
    final BookKeeperFactory bookKeeperFactory = mock(BookKeeperFactory.class);
    when(bookKeeperFactory.createBookKeeperClient(anyString(), ArgumentMatchers.<Configuration>any())).thenReturn(
        new RetryingBookkeeperClient(
            new TSocket("localhost", CacheConfig.getServerPort(conf), CacheConfig.getClientTimeout(conf)),
            CacheConfig.getMaxRetries(conf)));

    final WorkerBookKeeper.HeartbeatService heartbeatService = new WorkerBookKeeper.HeartbeatService(conf, bookKeeperFactory);
  }

  /**
   * Verify that the heartbeat service correctly makes a connection using a BookKeeper client after a number of retries.
   */
  @Test
  public void testHeartbeatRetryLogic_connectAfterRetries()
  {
    stopBookKeeperServer();
    startBookKeeperServerWithDelay(TEST_RETRY_INTERVAL * 2);

    final WorkerBookKeeper.HeartbeatService heartbeatService = new WorkerBookKeeper.HeartbeatService(conf, new BookKeeperFactory());
  }

  /**
   * Verify that the heartbeat service no longer attempts to connect once it runs out of retry attempts.
   *
   * @throws TTransportException if the BookKeeper client cannot be created.
   */
  @Test(expectedExceptions = RuntimeException.class)
  public void testHeartbeatRetryLogic_outOfRetries() throws TTransportException
  {
    final BookKeeperFactory bookKeeperFactory = mock(BookKeeperFactory.class);
    when(bookKeeperFactory.createBookKeeperClient(anyString(), ArgumentMatchers.<Configuration>any())).thenThrow(TTransportException.class);

    final WorkerBookKeeper.HeartbeatService heartbeatService = new WorkerBookKeeper.HeartbeatService(conf, bookKeeperFactory);
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
        BookKeeperServer.startServer(conf, new MetricRegistry());
      }
    };
    thread.start();

    while (!BookKeeperServer.isServerUp()) {
      Thread.sleep(200);
      log.info("Waiting for BookKeeper Server to come up");
    }
  }

  /**
   * Start an instance of the BookKeeper server with an initial delay.
   */
  private void startBookKeeperServerWithDelay(final int initialDelay)
  {
    final Thread thread = new Thread()
    {
      public void run()
      {
        try {
          Thread.sleep(initialDelay);
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        BookKeeperServer.startServer(conf, new MetricRegistry());
      }
    };
    thread.start();
  }

  /**
   * Stop the currently running BookKeeper server instance.
   */
  private void stopBookKeeperServer()
  {
    BookKeeperServer.stopServer();
  }
}
