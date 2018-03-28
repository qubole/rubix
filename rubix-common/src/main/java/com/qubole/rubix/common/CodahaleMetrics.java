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

package com.qubole.rubix.common;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;

import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.qubole.rubix.spi.CacheConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Abhishek on 2/14/18.
 */
public class CodahaleMetrics implements Metrics
{
  private static final Log log = LogFactory.getLog(CodahaleMetrics.class);

  public final MetricRegistry metricRegistry = new MetricRegistry();
  private final Lock countersLock = new ReentrantLock();
  private final Lock gaugesLock = new ReentrantLock();

  private LoadingCache<String, Counter> counters;
  private ConcurrentHashMap<String, Gauge> gauges;
  private final Set<Closeable> reporters = new HashSet<Closeable>();

  private Configuration conf;

  public CodahaleMetrics(Configuration conf)
  {
    this.conf = conf;
    counters = CacheBuilder.newBuilder().build(
        new CacheLoader<String, Counter>() {
          @Override
          public Counter load(String key)
          {
            Counter counter = new Counter();
            metricRegistry.register(key, counter);
            return counter;
          }
        });

    gauges = new ConcurrentHashMap<String, Gauge>();

    //register JVM metrics
    registerAll("rubix.bookkeeper.gc", new GarbageCollectorMetricSet());
    registerAll("rubix.bookkeeper.memory", new MemoryUsageGaugeSet());
    registerAll("rubix.bookkeeper.threads", new ThreadStatesGaugeSet());
    registerAll("rubix.bookkeeper.buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));

    //Metrics reporter
    Set<MetricsReporting> finalReporterList = new HashSet<MetricsReporting>();
    List<String> metricsReporterNames = Lists.newArrayList(
        Splitter.on(",").trimResults().omitEmptyStrings().split(CacheConfig.getMetricsReporters(conf)));

    if (metricsReporterNames != null) {
      for (String metricsReportingName : metricsReporterNames) {
        try {
          MetricsReporting reporter = MetricsReporting.valueOf(metricsReportingName.trim().toUpperCase());
          finalReporterList.add(reporter);
        }
        catch (IllegalArgumentException e) {
          log.warn("Metrics reporter skipped due to invalid configured reporter: " + metricsReportingName);
        }
      }
    }
    initReporting(finalReporterList);
  }

  private void initReporting(Set<MetricsReporting> reportingSet)
  {
    for (MetricsReporting reporting : reportingSet) {
      switch(reporting) {
        case JMX:
          final JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry)
              .convertRatesTo(TimeUnit.SECONDS)
              .convertDurationsTo(TimeUnit.MILLISECONDS)
              .build();
          jmxReporter.start();
          reporters.add(jmxReporter);
          break;
      }
    }
  }

  @Override
  public void close() throws Exception
  {
    if (reporters != null) {
      for (Closeable reporter : reporters) {
        reporter.close();
      }
    }
    for (Map.Entry<String, Metric> metric : metricRegistry.getMetrics().entrySet()) {
      metricRegistry.remove(metric.getKey());
    }
    counters.invalidateAll();
  }

  @Override
  public Long incrementCounter(String name)
  {
    return incrementCounter(name, 1L);
  }

  @Override
  public Long incrementCounter(String name, long increment)
  {
    String key = name;
    try {
      countersLock.lock();
      counters.get(key).inc(increment);
      return counters.get(key).getCount();
    }
    catch (ExecutionException ee) {
      throw new IllegalStateException("Error retrieving counter from the metric registry ", ee);
    }
    finally {
      countersLock.unlock();
    }
  }

  @Override
  public Long decrementCounter(String name)
  {
    return decrementCounter(name, 1L);
  }

  @Override
  public Long decrementCounter(String name, long decrement)
  {
    String key = name;
    try {
      countersLock.lock();
      counters.get(key).dec(decrement);
      return counters.get(key).getCount();
    }
    catch (ExecutionException ee) {
      throw new IllegalStateException("Error retrieving counter from the metric registry ", ee);
    }
    finally {
      countersLock.unlock();
    }
  }

  @Override
  public void addGauge(String name, final MetricsVariable variable)
  {
    Gauge gauge = new Gauge() {
      @Override
      public Object getValue()
      {
        return variable.getValue();
      }
    };
    addGaugeInternal(name, gauge);
  }

  private void addGaugeInternal(String name, Gauge gauge)
  {
    try {
      gaugesLock.lock();
      gauges.put(name, gauge);
      // Metrics throws an Exception if we don't do this when the key already exists
      if (metricRegistry.getGauges().containsKey(name)) {
        log.warn("A Gauge with name [" + name + "] already exists. "
            + " The old gauge will be overwritten, but this is not recommended");
        metricRegistry.remove(name);
      }
      metricRegistry.register(name, gauge);
    }
    finally {
      gaugesLock.unlock();
    }
  }

  private void registerAll(String prefix, MetricSet metricSet)
  {
    for (Map.Entry<String, Metric> entry : metricSet.getMetrics().entrySet()) {
      if (entry.getValue() instanceof MetricSet) {
        registerAll(prefix + "." + entry.getKey(), (MetricSet) entry.getValue());
      }
      else {
        metricRegistry.register(prefix + "." + entry.getKey().replace('.', '_'), entry.getValue());
      }
    }
  }
}
