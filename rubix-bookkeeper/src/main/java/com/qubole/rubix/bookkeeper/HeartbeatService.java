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

import com.google.common.base.Joiner;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Service;
import com.qubole.rubix.core.utils.ClusterUtil;
import com.qubole.rubix.core.utils.DataGen;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterType;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import com.qubole.rubix.spi.thrift.BlockLocation;
import com.qubole.rubix.spi.thrift.Location;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.TException;
import org.apache.thrift.shaded.transport.TTransportException;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HeartbeatService extends AbstractScheduledService
{
  private Log log = LogFactory.getLog(HeartbeatService.class.getName());

  // The name of the test file to be cached to verify the caching workflow.
  public static final String VALIDATOR_TEST_FILE_NAME = "rubixCachingBehaviorTestFile";

  // The path of the test file used to verify the caching workflow.
  public static final String VALIDATOR_TEST_FILE_PATH = Joiner.on(File.separator).join(System.getProperty("java.io.tmpdir"), VALIDATOR_TEST_FILE_NAME);

  // The path of the test file with a defined scheme (needed for BookKeeper service calls).
  public static final String VALIDATOR_TEST_FILE_PATH_WITH_SCHEME = "file://" + VALIDATOR_TEST_FILE_PATH;

  private static final int VALIDATOR_START_BLOCK = 0;
  private static final int VALIDATOR_END_BLOCK = 1;
  private static final int VALIDATOR_READ_OFFSET = 0;
  private static final int VALIDATOR_CLUSTER_TYPE = ClusterType.TEST_CLUSTER_MANAGER.ordinal();

  // The executor used for running listener callbacks.
  private final Executor executor = Executors.newSingleThreadExecutor();

  // The client for interacting with the master BookKeeper.
  private final RetryingBookkeeperClient bookkeeperClient;

  // The initial delay for sending heartbeats.
  private final int heartbeatInitialDelay;

  // The interval at which to send a heartbeat.
  private final int heartbeatInterval;

  // The current Hadoop configuration.
  private final Configuration conf;

  // The current BookKeeper instance.
  private final BookKeeper bookKeeper;

  // The hostname of the master node.
  private final String masterHostname;

  public HeartbeatService(Configuration conf, BookKeeperFactory bookKeeperFactory, BookKeeper bookKeeper)
  {
    this.conf = conf;
    this.heartbeatInitialDelay = CacheConfig.getHeartbeatInitialDelay(conf);
    this.heartbeatInterval = CacheConfig.getHeartbeatInterval(conf);
    this.masterHostname = ClusterUtil.getMasterHostname(conf);
    this.bookkeeperClient = initializeClientWithRetry(bookKeeperFactory);
    this.bookKeeper = bookKeeper;
  }

  /**
   * Attempt to initialize the client for communicating with the master BookKeeper.
   *
   * @param bookKeeperFactory   The factory to use for creating a BookKeeper client.
   * @return The client used for communication with the master node.
   */
  private RetryingBookkeeperClient initializeClientWithRetry(BookKeeperFactory bookKeeperFactory)
  {
    final int retryInterval = CacheConfig.getServiceRetryInterval(conf);
    final int maxRetries = CacheConfig.getServiceMaxRetries(conf);

    for (int failedStarts = 0; failedStarts < maxRetries; ) {
      try {
        RetryingBookkeeperClient client = bookKeeperFactory.createBookKeeperClient(masterHostname, conf);
        return client;
      }
      catch (TTransportException e) {
        failedStarts++;
        log.warn(String.format("Could not start client for heartbeat service [%d/%d attempts]", failedStarts, maxRetries));
      }

      if (failedStarts == maxRetries) {
        break;
      }

      try {
        Thread.sleep(retryInterval);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    log.fatal("Heartbeat service ran out of retries to connect to the master BookKeeper");
    throw new RuntimeException("Could not start heartbeat service");
  }

  @Override
  protected void startUp()
  {
    log.info(String.format("Starting service %s in thread %s", serviceName(), Thread.currentThread().getId()));
    addListener(new HeartbeatService.FailureListener(), executor);
  }

  @Override
  protected void runOneIteration()
  {
    try {
      boolean didValidationSucceed = validateCachingBehavior();

      log.debug(String.format("Sending heartbeat to %s", masterHostname));
      bookkeeperClient.handleHeartbeat(
          InetAddress.getLocalHost().getCanonicalHostName(),
          didValidationSucceed);
    }
    catch (IOException e) {
      log.error("Could not send heartbeat", e);
    }
    catch (TException te) {
      log.error(String.format("Could not connect to master node [%s]; will reattempt on next heartbeat", masterHostname));
    }
  }

  @Override
  protected AbstractScheduledService.Scheduler scheduler()
  {
    return AbstractScheduledService.Scheduler.newFixedDelaySchedule(heartbeatInitialDelay, heartbeatInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * Validate the behavior of the BookKeeper caching flow.
   *
   * @return true if caching behaves as expected, false otherwise
   */
  public boolean validateCachingBehavior()
  {
    try {
      DataGen.populateFile(VALIDATOR_TEST_FILE_PATH);
    }
    catch (IOException e) {
      log.error("Could not create temporary file for testing caching behavior", e);
      return false;
    }

    final File tempFile = new File(VALIDATOR_TEST_FILE_PATH);
    final long fileLength = tempFile.length();
    final long readSize = tempFile.length();
    final long fileLastModified = tempFile.lastModified();

    try {
      List<BlockLocation> locations = bookKeeper.getCacheStatus(
          VALIDATOR_TEST_FILE_PATH_WITH_SCHEME,
          fileLength,
          fileLastModified,
          VALIDATOR_START_BLOCK,
          VALIDATOR_END_BLOCK,
          VALIDATOR_CLUSTER_TYPE);
      if (locations.isEmpty() || locations.get(0).getLocation() != Location.LOCAL) {
        return false;
      }

      final boolean dataRead = bookKeeper.readData(
          VALIDATOR_TEST_FILE_PATH_WITH_SCHEME,
          VALIDATOR_READ_OFFSET,
          Ints.checkedCast(readSize),
          fileLength,
          fileLastModified,
          VALIDATOR_CLUSTER_TYPE);
      if (!dataRead) {
        return false;
      }

      locations = bookKeeper.getCacheStatus(
          VALIDATOR_TEST_FILE_PATH_WITH_SCHEME,
          fileLength,
          fileLastModified,
          VALIDATOR_START_BLOCK,
          VALIDATOR_END_BLOCK,
          VALIDATOR_CLUSTER_TYPE);
      if (locations.isEmpty() || locations.get(0).getLocation() != Location.CACHED) {
        return false;
      }

      return true;
    }
    catch (TException e) {
      log.error("Unable to validate caching behavior", e);
      return false;
    }
    finally {
      // Clean cache after validation
      BookKeeper.invalidateFileMetadata(VALIDATOR_TEST_FILE_PATH_WITH_SCHEME);
      tempFile.delete();
    }
  }

  /**
   * Listener to handle failures for this service.
   */
  private class FailureListener extends com.google.common.util.concurrent.Service.Listener
  {
    @Override
    public void failed(Service.State from, Throwable failure)
    {
      super.failed(from, failure);
      log.error("Encountered a problem", failure);
    }
  }
}
