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

import com.google.common.util.concurrent.AbstractScheduledService;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Class to send a heartbeat to the master node in the cluster.
 */
public class HeartbeatService extends AbstractScheduledService
{
  private static Log log = LogFactory.getLog(HeartbeatService.class.getName());

  // The executor used for running listener callbacks.
  private final Executor executor = Executors.newSingleThreadExecutor();

  // The initial delay for sending heartbeats.
  private final int heartbeatInitialDelay;

  // The interval at which to send a heartbeat.
  private final int heartbeatInterval;

  // The current Hadoop configuration.
  private final Configuration conf;

  // The hostname of the master node.
  private String masterHostname;

  public HeartbeatService(Configuration conf)
  {
    this.conf = conf;
    this.heartbeatInitialDelay = CacheConfig.getHeartbeatInitialDelay(conf);
    this.heartbeatInterval = CacheConfig.getHeartbeatInterval(conf);
    this.masterHostname = getMasterHostname();
  }

  @Override
  protected void startUp()
  {
    log.info("Starting service " + serviceName() + " in thread " + Thread.currentThread().getId());
    addListener(new FailureListener(), executor);
  }

  @Override
  protected void runOneIteration() throws IOException
  {
    RetryingBookkeeperClient client = null;
    try {
      client = new BookKeeperFactory().createBookKeeperClient(masterHostname, conf);
      log.info("Sending heartbeat to " + masterHostname);
      client.handleHeartbeat(InetAddress.getLocalHost().getCanonicalHostName());
    }
    catch (Exception e) {
      log.error("Could not send heartbeat", e);
    }
    finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Override
  protected Scheduler scheduler()
  {
    return Scheduler.newFixedDelaySchedule(heartbeatInitialDelay, heartbeatInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * Get the hostname for the master node in the cluster.
   *
   * @return The hostname of the master node, or <code>localhost</code> if the hostname could not be found.
   */
  private String getMasterHostname()
  {
    try {
      URI masterHostURI = new URI(conf.get("fs.defaultFS", "localhost"));
      return masterHostURI.getHost();
    }
    catch (URISyntaxException e) {
      log.error("Problem with syntax for master hostname", e);
    }
    return "localhost";
  }

  /**
   * Listener to handle failures for this service.
   */
  private static class FailureListener extends com.google.common.util.concurrent.Service.Listener
  {
    @Override
    public void failed(State from, Throwable failure)
    {
      super.failed(from, failure);
      log.error("Encountered a problem", failure);
    }
  }
}
