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
import com.google.common.base.Ticker;
import com.qubole.rubix.bookkeeper.exception.BookKeeperInitializationException;
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.spi.BookKeeperFactory;
import org.apache.hadoop.conf.Configuration;

public class MockWorkerBookKeeper extends WorkerBookKeeper
{
  public MockWorkerBookKeeper(Configuration conf, BookKeeperMetrics bookKeeperMetrics, Ticker ticker, BookKeeperFactory factory) throws BookKeeperInitializationException
  {
    super(conf, bookKeeperMetrics, ticker, factory);
  }

  @Override
  void startHeartbeatService(Configuration conf, MetricRegistry metrics, BookKeeperFactory factory)
  {
    if (conf.getBoolean("StartHeartBeatService", false)) {
      super.startHeartbeatService(conf, metrics, factory);
    }
  }

  @Override
  void setCurrentNodeName(Configuration conf)
  {
    nodeName = "localhost";
  }
}
