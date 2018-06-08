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

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Service;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.TException;
import org.apache.thrift.shaded.transport.TTransportException;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Class to manage components on a worker node.
 */
public class WorkerManager
{
  private static Log log = LogFactory.getLog(WorkerManager.class.getName());

  // The current Hadoop configuration.
  private final Configuration conf;

  public WorkerManager(Configuration conf)
  {
    this.conf = conf;
    startHeartbeatService();
  }

  /**
   * Start the {@link HeartbeatService} for this worker node.
   */
  public void startHeartbeatService()
  {
    try {
      HeartbeatService heartbeatService = new HeartbeatService(conf);
      heartbeatService.startAsync();
    }
    catch (TTransportException e) {
      log.fatal("Could not start heartbeat service", e);
    }
  }

  /**
   * Class to send a heartbeat to the master node in the cluster.
   */
  private static class HeartbeatService extends AbstractScheduledService
  {
    private static Log log = LogFactory.getLog(HeartbeatService.class.getName());

    private static final String KEY_MASTER_HOSTNAME = "master.hostname";
    private static final String KEY_YARN_RESOURCEMANAGER_ADDRESS = "yarn.resourcemanager.address";

    // The executor used for running listener callbacks.
    private final Executor executor = Executors.newSingleThreadExecutor();

    // The client for interacting with the master BookKeeper.
    private RetryingBookkeeperClient bookkeeperClient;

    // The initial delay for sending heartbeats.
    private final int heartbeatInitialDelay;

    // The interval at which to send a heartbeat.
    private final int heartbeatInterval;

    // The current Hadoop configuration.
    private final Configuration conf;

    // The hostname of the master node.
    private String masterHostname;

    public HeartbeatService(Configuration conf) throws TTransportException
    {
      this.conf = conf;
      this.heartbeatInitialDelay = CacheConfig.getHeartbeatInitialDelay(conf);
      this.heartbeatInterval = CacheConfig.getHeartbeatInterval(conf);
      this.masterHostname = getMasterHostname();
      this.bookkeeperClient = new BookKeeperFactory().createBookKeeperClient(masterHostname, conf);
    }

    @Override
    protected void startUp()
    {
      log.info(String.format("Starting service %s in thread %s", serviceName(), Thread.currentThread().getId()));
      addListener(new FailureListener(), executor);
    }

    @Override
    protected void runOneIteration() throws TException
    {
      try {
        log.info(String.format("Sending heartbeat to %s", masterHostname));
        bookkeeperClient.handleHeartbeat(InetAddress.getLocalHost().getCanonicalHostName());
      }
      catch (IOException e) {
        log.error("Could not send heartbeat", e);
      }
    }

    @Override
    protected AbstractScheduledService.Scheduler scheduler()
    {
      return AbstractScheduledService.Scheduler.newFixedDelaySchedule(heartbeatInitialDelay, heartbeatInterval, TimeUnit.MILLISECONDS);
    }

    /**
     * Get the hostname for the master node in the cluster.
     *
     * @return The hostname of the master node, or <code>localhost</code> if the hostname could not be found.
     */
    private String getMasterHostname()
    {
      // TODO move to common place (used in PrestoClusterManager)
      String host;

      log.debug("Trying master.hostname");
      host = conf.get(KEY_MASTER_HOSTNAME);
      if (host != null) {
        return host;
      }

      log.debug("Trying yarn.resourcemanager.address");
      host = conf.get(KEY_YARN_RESOURCEMANAGER_ADDRESS);
      if (host != null) {
        return host.substring(0, host.indexOf(":"));
      }

      log.debug("No hostname found in etc/*-site.xml, returning localhost");
      return "localhost";
    }

    /**
     * Listener to handle failures for this service.
     */
    private static class FailureListener extends com.google.common.util.concurrent.Service.Listener
    {
      @Override
      public void failed(Service.State from, Throwable failure)
      {
        super.failed(from, failure);
        log.error("Encountered a problem", failure);
      }
    }
  }
}
