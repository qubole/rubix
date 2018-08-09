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

import com.qubole.rubix.bookkeeper.test.BookKeeperTestUtils;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class TestValidatorFileVisitor
{
  private static final Log log = LogFactory.getLog(TestValidatorFileVisitor.class);

  private static final String TEST_CACHE_DIR_PREFIX = BookKeeperTestUtils.getTestCacheDirPrefix("TestValidatorFileVisitor");
  private static final int TEST_MAX_DISKS = 1;

  private final Configuration conf = new Configuration();

  @BeforeMethod
  public void setUp() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);
    CacheConfig.setMaxDisks(conf, TEST_MAX_DISKS);

    BookKeeperTestUtils.createCacheParentDirectories(conf, TEST_MAX_DISKS);
  }

  @AfterMethod
  public void tearDown() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);
    BookKeeperTestUtils.removeCacheParentDirectories(conf, TEST_MAX_DISKS);

    conf.clear();
  }

  @Test
  public void testValidatorFileVisitor_allMdExists() throws IOException
  {
    final int maxDepth = 1;
    final int maxDirs = 0;
    final int maxFiles = 10;
    final int mdSkip = 1;

    runAndVerifyValidator(conf, maxDepth, maxDirs, maxFiles, mdSkip);
  }

  @Test
  public void testValidatorFileVisitor_mdDoesNotExist() throws IOException
  {
    final int maxDepth = 1;
    final int maxDirs = 0;
    final int maxFiles = 10;
    final int mdSkip = 10;

    runAndVerifyValidator(conf, maxDepth, maxDirs, maxFiles, mdSkip);
  }

  @Test
  public void testValidatorFileVisitor_someMdExists() throws IOException
  {
    final int maxDepth = 1;
    final int maxDirs = 0;
    final int maxFiles = 10;
    final int mdSkip = 5;

    runAndVerifyValidator(conf, maxDepth, maxDirs, maxFiles, mdSkip);
  }

  @Test
  public void testValidatorFileVisitor_allMdExists_multiDepth() throws IOException
  {
    final int maxDepth = 3;
    final int maxDirs = 5;
    final int maxFiles = 10;
    final int mdSkip = 1;

    runAndVerifyValidator(conf, maxDepth, maxDirs, maxFiles, mdSkip);
  }

  @Test
  public void testValidatorFileVisitor_mdDoesNotExist_multiDepth() throws IOException
  {
    final int maxDepth = 3;
    final int maxDirs = 5;
    final int maxFiles = 10;
    final int mdSkip = 10;

    runAndVerifyValidator(conf, maxDepth, maxDirs, maxFiles, mdSkip);
  }

  @Test
  public void testValidatorFileVisitor_someMdExists_multiDepth() throws IOException
  {
    final int maxDepth = 3;
    final int maxDirs = 5;
    final int maxFiles = 10;
    final int mdSkip = 5;

    runAndVerifyValidator(conf, maxDepth, maxDirs, maxFiles, mdSkip);
  }

  /**
   * Run and verify a {@link ValidatorFileVisitor}
   *
   * @param conf    The current Hadoop configuration.
   * @param depth   The depth to which directories will be created.
   * @param dirs    The number of directories to create at each depth.
   * @param files   The number of files to create at each depth.
   * @param mdStep  The frequency at which to generate metadata files.
   * @throws IOException if an I/O error occurs while creating files.
   */
  private void runAndVerifyValidator(Configuration conf, int depth, int dirs, int files, int mdStep) throws IOException
  {
    ValidatorFileGen.FileGenResult fileGenResult = ValidatorFileGen.generateTestFiles(conf, depth, dirs, files, mdStep);
    ValidationResult result = validate();

    verifyCorrectness(fileGenResult, result);
  }

  /**
   * Run a {@link ValidatorFileVisitor} on all configured cache directories.
   *
   * @return The result of the validation.
   * @throws IOException if an I/O error occurs while visiting files.
   */
  private ValidationResult validate() throws IOException
  {
    final ValidatorFileVisitor validator = new ValidatorFileVisitor(conf);

    final Map<Integer, String> diskMap = CacheUtil.getCacheDiskPathsMap(conf);
    for (int disk = 0; disk < diskMap.size(); disk++) {
      Files.walkFileTree(Paths.get(diskMap.get(disk)), validator);
    }

    return validator.getResult();
  }

  /**
   * Verify the correctness of the {@link ValidatorFileVisitor}
   *
   * @param fileGenResult     The validationResult of the test file generation.
   * @param validationResult  The validationResult of the file validation.
   */
  private void verifyCorrectness(ValidatorFileGen.FileGenResult fileGenResult, ValidationResult validationResult)
  {
    assertEquals(validationResult.getSuccesses(), fileGenResult.getTotalMDFilesCreated());
    assertEquals(validationResult.getTotalFiles(), fileGenResult.getTotalCacheFilesCreated());
    assertEquals(validationResult.getFilesWithoutMD(), fileGenResult.getFilesWithoutMd());
    assertEquals(validationResult.getSuccessRate(), fileGenResult.getSuccessRate());
  }
}
