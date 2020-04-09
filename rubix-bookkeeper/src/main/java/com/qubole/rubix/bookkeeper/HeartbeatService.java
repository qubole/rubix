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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.qubole.rubix.bookkeeper.validation.CachingValidator;
import com.qubole.rubix.bookkeeper.validation.FileValidator;
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.common.utils.ClusterUtil;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.RetryingPooledBookkeeperClient;
import com.qubole.rubix.spi.thrift.HeartbeatStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.TException;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HeartbeatService extends AbstractScheduledService
{
  private Log log = LogFactory.getLog(HeartbeatService.class.getName());

  // The executor used for running listener callbacks.
  private final ScheduledExecutorService validatorExecutor = Executors.newSingleThreadScheduledExecutor();

  // The client for interacting with the master BookKeeper.
  private final RetryingPooledBookkeeperClient bookkeeperClient;

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

  private FileValidator fileValidator;
  private CachingValidator cachingValidator;

  public HeartbeatService(Configuration conf, MetricRegistry metrics, BookKeeperFactory bookKeeperFactory, BookKeeper bookKeeper)
  {
    this.conf = conf;
    this.bookKeeper = bookKeeper;
    this.heartbeatInitialDelay = CacheConfig.getHeartbeatInitialDelay(conf);
    this.heartbeatInterval = CacheConfig.getHeartbeatInterval(conf);
    this.masterHostname = ClusterUtil.getMasterHostname(conf);
    this.bookkeeperClient = initializeClientWithRetry(bookKeeperFactory);

    if (CacheConfig.isValidationEnabled(conf)) {
      this.cachingValidator = new CachingValidator(conf, bookKeeper, validatorExecutor);
      this.cachingValidator.startAsync();
      metrics.register(BookKeeperMetrics.ValidationMetric.CACHING_VALIDATION_SUCCESS_GAUGE.getMetricName(), new Gauge<Integer>()
      {
        @Override
        public Integer getValue()
        {
          return cachingValidator.didValidationSucceed() ? 1 : 0;
        }
      });

      this.fileValidator = new FileValidator(conf, validatorExecutor);
      this.fileValidator.startAsync();
      metrics.register(BookKeeperMetrics.ValidationMetric.FILE_VALIDATION_SUCCESS_GAUGE.getMetricName(), new Gauge<Integer>()
      {
        @Override
        public Integer getValue()
        {
          return fileValidator.didValidationSucceed() ? 1 : 0;
        }
      });
    }
  }

  /**
   * Attempt to initialize the client for communicating with the master BookKeeper.
   *
   * @param bookKeeperFactory   The factory to use for creating a BookKeeper client.
   * @return The client used for communication with the master node.
   */
  private RetryingPooledBookkeeperClient initializeClientWithRetry(BookKeeperFactory bookKeeperFactory)
  {
    final int retryInterval = CacheConfig.getServiceRetryInterval(conf);
    final int maxRetries = CacheConfig.getServiceMaxRetries(conf);

    for (int failedStarts = 0; failedStarts < maxRetries; ) {
      try {
        RetryingPooledBookkeeperClient client = bookKeeperFactory.createBookKeeperClient(masterHostname, conf);
        return client;
      }
      catch (Exception e) {
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
    log.info(String.format("Starting service %s in thread %d", serviceName(), Thread.currentThread().getId()));
    addListener(new HeartbeatService.FailureListener(), MoreExecutors.directExecutor());
  }

  @Override
  protected void runOneIteration()
  {
    try {
      HeartbeatStatus status = CacheConfig.isValidationEnabled(conf)
          ? new HeartbeatStatus(fileValidator.didValidationSucceed(), cachingValidator.didValidationSucceed())
          : new HeartbeatStatus();

      log.debug(String.format("Sending heartbeat to %s", masterHostname));
      bookkeeperClient.handleHeartbeat(InetAddress.getLocalHost().getCanonicalHostName(), status);
    }
    catch (IOException e) {
      log.error("Could not send heartbeat", e);
    }
    catch (TException te) {
      log.error(String.format("Could not connect to master node [%s]; will reattempt on next heartbeat", masterHostname));
    }
  }

  @Override
  protected Scheduler scheduler()
  {
    return Scheduler.newFixedDelaySchedule(heartbeatInitialDelay, heartbeatInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * Listener to handle failures for this service.
   */
  private class FailureListener extends Service.Listener
  {
    @Override
    public void failed(Service.State from, Throwable failure)
    {
      super.failed(from, failure);
      log.error("Encountered a problem", failure);
    }
  }
}
