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
package com.qubole.rubix.bookkeeper.validation;

import com.google.common.base.Joiner;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.qubole.rubix.bookkeeper.BookKeeper;
import com.qubole.rubix.common.utils.DataGen;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.ClusterType;
import com.qubole.rubix.spi.thrift.BlockLocation;
import com.qubole.rubix.spi.thrift.CacheStatusRequest;
import com.qubole.rubix.spi.thrift.CacheStatusResponse;
import com.qubole.rubix.spi.thrift.Location;
import com.qubole.rubix.spi.thrift.ReadResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.TException;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class CachingValidator extends AbstractScheduledService
{
  private Log log = LogFactory.getLog(CachingValidator.class);

  // The name of the test file to be cached to verify the caching workflow.
  public static final String VALIDATOR_TEST_FILE_NAME = "rubixCachingTestFile";

  // The path of the test file used to verify the caching workflow.
  public static final String VALIDATOR_TEST_FILE_PATH = Joiner.on(File.separator).join(System.getProperty("java.io.tmpdir"), VALIDATOR_TEST_FILE_NAME);

  // The path of the test file with a defined scheme (needed for BookKeeper service calls).
  public static final String VALIDATOR_TEST_FILE_PATH_WITH_SCHEME = "file://" + VALIDATOR_TEST_FILE_PATH;

  private static final int VALIDATOR_START_BLOCK = 0;
  private static final int VALIDATOR_END_BLOCK = 1;
  private static final int VALIDATOR_READ_OFFSET = 0;
  private static final int VALIDATOR_CLUSTER_TYPE = ClusterType.TEST_CLUSTER_MANAGER.ordinal();

  private final BookKeeper bookKeeper;
  private final ScheduledExecutorService validationExecutor;
  private final int cachingValidationInterval;

  private AtomicBoolean validationSuccess = new AtomicBoolean(true);

  public CachingValidator(Configuration conf, BookKeeper bookKeeper, ScheduledExecutorService validationExecutor)
  {
    this.bookKeeper = bookKeeper;
    this.validationExecutor = validationExecutor;
    this.cachingValidationInterval = CacheConfig.getCachingValidationInterval(conf);
  }

  @Override
  protected ScheduledExecutorService executor()
  {
    return validationExecutor;
  }

  @Override
  protected void runOneIteration()
  {
    validationSuccess.set(validateCachingBehavior());
  }

  @Override
  protected Scheduler scheduler()
  {
    return Scheduler.newFixedDelaySchedule(0, cachingValidationInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * Get the success of the caching validation.
   *
   * @return true if caching validation succeeded, false otherwise.
   */
  public boolean didValidationSucceed()
  {
    return validationSuccess.get();
  }

  /**
   * Validate the behavior of the BookKeeper caching flow.
   *
   * @return true if caching behaves as expected, false otherwise
   */
  protected boolean validateCachingBehavior()
  {
    try {
      DataGen.populateFile(VALIDATOR_TEST_FILE_PATH);
    }
    catch (IOException e) {
      log.error("Could not create temporary file for testing caching behavior", e);
      return false;
    }

    final File tempFile = new File(VALIDATOR_TEST_FILE_PATH);
    final long fileLength = tempFile.length();
    final long readSize = tempFile.length();
    final long fileLastModified = tempFile.lastModified();

    CacheStatusRequest request = new CacheStatusRequest(VALIDATOR_TEST_FILE_PATH_WITH_SCHEME, fileLength,
        fileLastModified, VALIDATOR_START_BLOCK, VALIDATOR_END_BLOCK).setClusterType(VALIDATOR_CLUSTER_TYPE);

    try {
      CacheStatusResponse response = bookKeeper.getCacheStatus(request);
      if (response == null || response.getBlocks().isEmpty() || response.getBlocks().get(0).getLocation() != Location.LOCAL) {
        return false;
      }

      final boolean dataRead = bookKeeper.readData(
          VALIDATOR_TEST_FILE_PATH_WITH_SCHEME,
          VALIDATOR_READ_OFFSET,
          Ints.checkedCast(readSize),
          fileLength,
          fileLastModified,
          VALIDATOR_CLUSTER_TYPE).isStatus();
      if (!dataRead) {
        return false;
      }

      List<BlockLocation> locations = bookKeeper.getCacheStatus(request).getBlocks();
      if (locations.isEmpty() || locations.get(0).getLocation() != Location.CACHED) {
        return false;
      }

      return true;
    }
    catch (TException e) {
      log.error("Unable to validate caching behavior", e);
      return false;
    }
    finally {
      // Clean cache after validation
      bookKeeper.invalidateFileMetadata(VALIDATOR_TEST_FILE_PATH_WITH_SCHEME);
      tempFile.delete();
    }
  }
}
