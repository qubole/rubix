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
package com.qubole.rubix.common.metrics;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.qubole.rubix.spi.CacheConfig;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;

public class BookKeeperMetricsFilter implements MetricFilter
{
  private final List<String> whitelist;

  public BookKeeperMetricsFilter(Configuration conf)
  {
    this.whitelist = createWhitelist(conf);
  }

  private List<String> createWhitelist(Configuration conf)
  {
    List<String> whitelist = new ArrayList<>();
    if (CacheConfig.areHealthMetricsEnabled(conf)) {
      whitelist.addAll(BookKeeperMetrics.HealthMetric.getAllNames());
    }
    if (CacheConfig.areCacheMetricsEnabled(conf)) {
      whitelist.addAll(BookKeeperMetrics.CacheMetric.getAllNames());
    }
    if (CacheConfig.areJvmMetricsEnabled(conf)) {
      whitelist.addAll(BookKeeperMetrics.BookKeeperJvmMetric.getAllNames());
      whitelist.addAll(BookKeeperMetrics.LDTSJvmMetric.getAllNames());
    }
    if (CacheConfig.isValidationEnabled(conf)) {
      whitelist.addAll(BookKeeperMetrics.ValidationMetric.getAllNames());
    }

    return whitelist;
  }

  @Override
  public boolean matches(String name, Metric metric)
  {
    for (String whitelistedMetric : whitelist) {
      if (name.startsWith(whitelistedMetric)) {
        return true;
      }
    }
    return false;
  }
}
