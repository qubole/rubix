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
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.thrift.HeartbeatStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.FileNotFoundException;

public class WorkerBookKeeper extends BookKeeper
{
  private static Log log = LogFactory.getLog(WorkerBookKeeper.class);
  private HeartbeatService heartbeatService;

  public WorkerBookKeeper(Configuration conf, BookKeeperMetrics bookKeeperMetrics) throws FileNotFoundException
  {
    this(conf, bookKeeperMetrics, new BookKeeperFactory());
  }

  public WorkerBookKeeper(Configuration conf, BookKeeperMetrics bookKeeperMetrics, BookKeeperFactory factory) throws FileNotFoundException
  {
    super(conf, bookKeeperMetrics);
    startHeartbeatService(conf, metrics, factory);
  }

  @Override
  public void handleHeartbeat(String workerHostname, HeartbeatStatus request)
  {
    throw new UnsupportedOperationException("Worker node should not handle heartbeat");
  }

  /**
   * Start the {@link HeartbeatService} for this worker node.
   */
  private void startHeartbeatService(Configuration conf, MetricRegistry metrics, BookKeeperFactory factory)
  {
    if (CacheConfig.isHeartbeatEnabled(conf) || !CacheConfig.isEmbeddedModeEnabled(conf)) {
      this.heartbeatService = new HeartbeatService(conf, metrics, factory, this);
      heartbeatService.startAsync();
    }
  }
}
