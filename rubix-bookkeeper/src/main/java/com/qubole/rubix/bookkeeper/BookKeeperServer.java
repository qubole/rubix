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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.qubole.rubix.spi.BookKeeperService;
import com.qubole.rubix.spi.CacheConfig;
import com.readytalk.metrics.StatsDReporter;
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
import java.util.concurrent.TimeUnit;

import static com.qubole.rubix.spi.CacheConfig.getServerMaxThreads;
import static com.qubole.rubix.spi.CacheConfig.getServerPort;

/**
 * Created by stagra on 15/2/16.
 */
public class BookKeeperServer extends Configured implements Tool
{
  // Metric key for liveness of the BookKeeper daemon.
  public static final String METRIC_BOOKKEEPER_LIVENESS_CHECK = "rubix.bookkeeper.liveness.gauge";

  public static BookKeeper bookKeeper;
  public static BookKeeperService.Processor processor;

  // Registry for gathering & storing necessary metrics
  private static MetricRegistry metrics;

  public static Configuration conf;

  private static TServer server;

  private static Log log = LogFactory.getLog(BookKeeperServer.class.getName());

  private BookKeeperServer()
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

  public static void startServer(Configuration conf, MetricRegistry metricsRegistry)
  {
    metrics = metricsRegistry;
    try {
      bookKeeper = new BookKeeper(conf, metrics);
    }
    catch (FileNotFoundException e) {
      log.error("Cache directories could not be created", e);
      return;
    }

    registerMetrics(conf);

    DiskMonitorService diskMonitorService = new DiskMonitorService(conf, bookKeeper);
    diskMonitorService.startAsync();
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
  private static void registerMetrics(Configuration conf)
  {
    if ((CacheConfig.isOnMaster(conf) && CacheConfig.isReportStatsdMetricsOnMaster(conf))
        || (!CacheConfig.isOnMaster(conf) && CacheConfig.isReportStatsdMetricsOnWorker(conf))) {
      log.info("Reporting metrics to StatsD");
      StatsDReporter.forRegistry(metrics)
          .build(CacheConfig.getStatsDMetricsHost(conf), CacheConfig.getStatsDMetricsPort(conf))
          .start(CacheConfig.getStatsDMetricsInterval(conf), TimeUnit.MILLISECONDS);
    }

    metrics.register(METRIC_BOOKKEEPER_LIVENESS_CHECK, new Gauge<Integer>()
    {
      @Override
      public Integer getValue()
      {
        return 1;
      }
    });
  }

  public static void stopServer()
  {
    metrics.remove(METRIC_BOOKKEEPER_LIVENESS_CHECK);
    server.stop();
  }

  @VisibleForTesting
  public static boolean isServerUp()
  {
    if (server != null) {
      return server.isServing();
    }

    return false;
  }
}
