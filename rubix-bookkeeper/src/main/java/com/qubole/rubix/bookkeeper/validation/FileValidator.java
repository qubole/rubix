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

import com.google.common.util.concurrent.AbstractScheduledService;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public class FileValidator extends AbstractScheduledService
{
  private static final Log log = LogFactory.getLog(FileValidator.class);

  private final Configuration conf;
  private final int validationInterval;

  private FileValidatorResult validatorResult = new FileValidatorResult();

  public FileValidator(Configuration conf)
  {
    this.conf = conf;
    this.validationInterval = CacheConfig.getCachingValidationInterval(conf);
  }

  @Override
  protected void runOneIteration() throws Exception
  {
    validatorResult = validateCache();
  }

  @Override
  protected Scheduler scheduler()
  {
    return Scheduler.newFixedDelaySchedule(0, validationInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * Get the success of the file validation.
   *
   * @return true if file validation succeeded, false otherwise.
   */
  public boolean didValidationSucceed()
  {
    return validatorResult.getFailureCount() == 0;
  }

  /**
   * Validate all configured cache directories.
   *
   * @return The result of the cache validation.
   */
  private FileValidatorResult validateCache()
  {
    final int maxDisks = CacheUtil.getCacheDiskCount(conf);

    final FileValidatorResult allDisksResult = new FileValidatorResult();
    for (int diskIndex = 0; diskIndex < maxDisks; diskIndex++) {
      final FileValidatorVisitor validatorVisitor = new FileValidatorVisitor(conf);

      final Path diskCachePath = Paths.get(CacheUtil.getDirPath(diskIndex, conf), CacheConfig.getCacheDataDirSuffix(conf));
      try {
        Files.walkFileTree(diskCachePath, validatorVisitor);
      }
      catch (IOException e) {
        log.error("Encountered issue while verifying files", e);
      }

      allDisksResult.addResult(validatorVisitor.getResult());
    }

    if (allDisksResult.getFailureCount() > 0) {
      log.error("Validation Error!");
      log.error("The following cache files do not have an associated metadata file:");

      for (String fileName : allDisksResult.getFilesWithoutMD()) {
        log.error(String.format("-- %s", fileName));
      }
    }

    return allDisksResult;
  }
}
