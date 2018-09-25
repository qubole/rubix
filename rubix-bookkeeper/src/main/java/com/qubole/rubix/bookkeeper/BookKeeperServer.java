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
import com.codahale.metrics.jvm.CachedThreadStatesGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.thrift.BookKeeperService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.thrift.shaded.server.TServer;
import org.apache.thrift.shaded.server.TThreadPoolServer;
import org.apache.thrift.shaded.transport.TServerSocket;
import org.apache.thrift.shaded.transport.TServerTransport;
import org.apache.thrift.shaded.transport.TTransportException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.qubole.rubix.spi.CacheConfig.getServerMaxThreads;
import static com.qubole.rubix.spi.CacheConfig.getServerPort;

/**
 * Created by stagra on 15/2/16.
 */
public class BookKeeperServer extends Configured implements Tool
{
  public BookKeeper bookKeeper;
  public BookKeeperService.Processor processor;

  // Registry for gathering & storing necessary metrics
  protected MetricRegistry metrics;

  public Configuration conf;

  private TServer server;

  private static Log log = LogFactory.getLog(BookKeeperServer.class.getName());
  private BookKeeperMetrics bookKeeperMetrics;

  public BookKeeperServer()
  {
  }

  public static void main(String[] args) throws Exception
  {
    ToolRunner.run(new Configuration(), new BookKeeperServer(), args);
  }

  @Override
  public int run(String[] args) throws Exception
  {
    conf = this.getConf();
    Runnable bookKeeperServer = new Runnable()
    {
      public void run()
      {
        startServer(conf, new MetricRegistry());
      }
    };
    new Thread(bookKeeperServer).run();
    return 0;
  }

  public void startServer(Configuration conf, MetricRegistry metricsRegistry)
  {
    metrics = metricsRegistry;
    try {
      if (CacheConfig.isOnMaster(conf)) {
        bookKeeper = new CoordinatorBookKeeper(conf, metrics);
      }
      else {
        bookKeeper = new WorkerBookKeeper(conf, metrics);
      }
    }
    catch (FileNotFoundException e) {
      log.error("Cache directories could not be created", e);
      return;
    }

    registerMetrics(conf);

    processor = new BookKeeperService.Processor(bookKeeper);
    log.info("Starting BookKeeperServer on port " + getServerPort(conf));
    try {
      TServerTransport serverTransport = new TServerSocket(getServerPort(conf));
      server = new TThreadPoolServer(new TThreadPoolServer
          .Args(serverTransport)
          .processor(processor)
          .maxWorkerThreads(getServerMaxThreads(conf)));

      server.serve();
    }
    catch (TTransportException e) {
      e.printStackTrace();
      log.error(String.format("Error starting BookKeeper server %s", Throwables.getStackTraceAsString(e)));
    }
  }

  /**
   * Register desired metrics.
   */
  protected void registerMetrics(Configuration conf)
  {
    bookKeeperMetrics = new BookKeeperMetrics(conf, metrics);

    metrics.register(BookKeeperMetrics.BookKeeperJvmMetric.BOOKKEEPER_JVM_GC_PREFIX.getMetricName(), new GarbageCollectorMetricSet());
    metrics.register(BookKeeperMetrics.BookKeeperJvmMetric.BOOKKEEPER_JVM_THREADS_PREFIX.getMetricName(), new CachedThreadStatesGaugeSet(CacheConfig.getStatsDMetricsInterval(conf), TimeUnit.MILLISECONDS));
    metrics.register(BookKeeperMetrics.BookKeeperJvmMetric.BOOKKEEPER_JVM_MEMORY_PREFIX.getMetricName(), new MemoryUsageGaugeSet());
  }

  public void stopServer()
  {
    removeMetrics();
    try {
      bookKeeperMetrics.close();
    }
    catch (IOException e) {
      log.error("Metrics reporters could not be closed", e);
    }
    server.stop();
  }

  protected void removeMetrics()
  {
    metrics.removeMatching(bookKeeperMetrics.getMetricsFilter());
  }

  @VisibleForTesting
  public boolean isServerUp()
  {
    if (server != null) {
      return server.isServing();
    }

    return false;
  }
}
