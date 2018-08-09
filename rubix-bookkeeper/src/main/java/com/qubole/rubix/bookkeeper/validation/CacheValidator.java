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
package com.qubole.rubix.bookkeeper.validation;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class CacheValidator extends AbstractScheduledService
{
  private static final Log log = LogFactory.getLog(CacheValidator.class);

  private static final String METRIC_BOOKKEEPER_CACHE_SUCCESS_RATE = "rubix.bookkeeper.validation_success_rate.gauge";

  private final Configuration conf;
  private final MetricRegistry metrics;
  private final int validationInitialDelay;
  private final int validationInterval;

  private ValidationResult cacheDirectoryValidationResult;

  public CacheValidator(Configuration conf, MetricRegistry metrics)
  {
    this.conf = conf;
    this.metrics = metrics;

    this.validationInitialDelay = CacheConfig.getCacheValidationInitialDelay(conf);
    this.validationInterval = CacheConfig.getCacheValidationInterval(conf);

    registerMetrics();
  }

  @Override
  protected void runOneIteration() throws Exception
  {
    cacheDirectoryValidationResult = validateCache();
  }

  @Override
  protected Scheduler scheduler()
  {
    return Scheduler.newFixedDelaySchedule(validationInitialDelay, validationInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * Register desired metrics.
   */
  private void registerMetrics()
  {
    metrics.register(METRIC_BOOKKEEPER_CACHE_SUCCESS_RATE, new Gauge<Double>()
    {
      @Override
      public Double getValue()
      {
        return cacheDirectoryValidationResult.getSuccessRate();
      }
    });
  }

  /**
   * Validate all configured cache directories.
   *
   * @return The result of the cache validation.
   * @throws IOException if an I/O error occurs while visiting files.
   */
  private ValidationResult validateCache() throws IOException
  {
    final int maxDisks = CacheConfig.getCacheMaxDisks(conf);

    final ValidationResult allDisksResult = new ValidationResult(0, 0, new HashSet<String>());
    for (int diskIndex = 0; diskIndex < maxDisks; diskIndex++) {
      ValidatorFileVisitor validatorVisitor = new ValidatorFileVisitor(conf);
      Files.walkFileTree(Paths.get(CacheUtil.getDirPath(diskIndex, conf)), validatorVisitor);

      allDisksResult.addResult(validatorVisitor.getResult());
    }

    return allDisksResult;
  }
}
