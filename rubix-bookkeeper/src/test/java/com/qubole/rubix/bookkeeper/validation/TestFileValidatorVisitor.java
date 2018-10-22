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

import com.codahale.metrics.MetricRegistry;
import com.qubole.rubix.bookkeeper.BaseServerTest;
import com.qubole.rubix.bookkeeper.BookKeeper;
import com.qubole.rubix.bookkeeper.WorkerBookKeeper;
import com.qubole.rubix.common.TestUtil;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import com.qubole.rubix.spi.ClusterType;
import com.qubole.rubix.spi.thrift.CacheStatusRequest;
import com.qubole.rubix.spi.thrift.UpdateCacheRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.TException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestFileValidatorVisitor
{
  private static final Log log = LogFactory.getLog(TestFileValidatorVisitor.class);

  private static final String TEST_CACHE_DIR_PREFIX = TestUtil.getTestCacheDirPrefix("TestFileValidatorVisitor");
  private static final int TEST_MAX_DISKS = 1;
  private static final int TEST_BLOCK_SIZE = 100;

  private static final String TEST_REMOTE_PATH_ONE = "/tmp/testPath1";
  private static final String TEST_REMOTE_PATH_TWO = "/tmp/testPath2";
  private static final long TEST_LAST_MODIFIED = 1514764800; // 2018-01-01T00:00:00
  private static final long TEST_FILE_LENGTH = 5000;
  private static final long TEST_START_BLOCK = 20;
  private static final long TEST_END_BLOCK = 23;

  private final Configuration conf = new Configuration();
  MetricRegistry metric;
  BookKeeper bookKeeper;

  @BeforeMethod
  public void setUp() throws IOException, InterruptedException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);
    CacheConfig.setMaxDisks(conf, TEST_MAX_DISKS);
    CacheConfig.setBlockSize(conf, TEST_BLOCK_SIZE);
    metric = new MetricRegistry();
    TestUtil.createCacheParentDirectories(conf, TEST_MAX_DISKS);
    CacheConfig.setValidationEnabled(conf, false);

    BaseServerTest.startCoordinatorBookKeeperServer(conf, new MetricRegistry());
    bookKeeper = new WorkerBookKeeper(conf, metric);
  }

  @AfterMethod
  public void tearDown() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);
    TestUtil.removeCacheParentDirectories(conf, TEST_MAX_DISKS);

    BaseServerTest.stopBookKeeperServer();

    conf.clear();
    metric = null;
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

  @Test
  public void testValidatorFileVisitor_allValidCachedFiles() throws TException, IOException
  {
    String filePath = CacheUtil.getLocalPath(TEST_REMOTE_PATH_ONE, conf);
    Files.createFile(Paths.get(filePath));
    log.info("FilePath " + filePath);

    filePath = CacheUtil.getLocalPath(TEST_REMOTE_PATH_TWO, conf);
    Files.createFile(Paths.get(filePath));
    log.info("FilePath " + filePath);

    CacheStatusRequest request = new CacheStatusRequest(TEST_REMOTE_PATH_ONE, TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
        TEST_START_BLOCK, TEST_END_BLOCK, ClusterType.TEST_CLUSTER_MANAGER.ordinal());
    bookKeeper.getCacheStatus(request);

    UpdateCacheRequest updateRequest = new UpdateCacheRequest(TEST_REMOTE_PATH_ONE, TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
        TEST_START_BLOCK, TEST_END_BLOCK, 300);
    bookKeeper.setAllCached(updateRequest);

    request = new CacheStatusRequest(TEST_REMOTE_PATH_TWO, TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
        TEST_START_BLOCK, TEST_END_BLOCK, ClusterType.TEST_CLUSTER_MANAGER.ordinal());
    bookKeeper.getCacheStatus(request);

    updateRequest = new UpdateCacheRequest(TEST_REMOTE_PATH_TWO, TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
        TEST_START_BLOCK, TEST_END_BLOCK, 300);
    bookKeeper.setAllCached(updateRequest);

    FileValidatorResult result = validate();

    assertTrue(result.getTotalFiles() == 2, "Total file count didn't match");
    assertTrue(result.getCorruptedCachedFiles().size() == 0, " Corrupted file found");
    assertTrue(result.getSuccessCount() == 2, "Not all cached files are in consistent state");
  }

  @Test
  public void testValidatorFileVisitor_allCorruptedFiles() throws TException, IOException
  {
    String filePath = CacheUtil.getLocalPath(TEST_REMOTE_PATH_ONE, conf);
    Files.createFile(Paths.get(filePath));
    log.info("FilePath " + filePath);

    filePath = CacheUtil.getLocalPath(TEST_REMOTE_PATH_TWO, conf);
    Files.createFile(Paths.get(filePath));
    log.info("FilePath " + filePath);

    CacheStatusRequest request = new CacheStatusRequest(TEST_REMOTE_PATH_ONE, TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
        TEST_START_BLOCK, TEST_END_BLOCK, ClusterType.TEST_CLUSTER_MANAGER.ordinal());
    bookKeeper.getCacheStatus(request);

    UpdateCacheRequest updateRequest = new UpdateCacheRequest(TEST_REMOTE_PATH_ONE, TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
        TEST_START_BLOCK, TEST_END_BLOCK, 200);
    bookKeeper.setAllCached(updateRequest);

    request = new CacheStatusRequest(TEST_REMOTE_PATH_TWO, TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
        TEST_START_BLOCK, TEST_END_BLOCK, ClusterType.TEST_CLUSTER_MANAGER.ordinal());
    bookKeeper.getCacheStatus(request);

    updateRequest = new UpdateCacheRequest(TEST_REMOTE_PATH_TWO, TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
        TEST_START_BLOCK, TEST_END_BLOCK, 100);
    bookKeeper.setAllCached(updateRequest);

    FileValidatorResult result = validate();

    assertTrue(result.getTotalFiles() == 2, "Total file count didn't match");
    assertTrue(result.getCorruptedCachedFiles().size() == 2, "All çached file should be in corrupted state");
    assertTrue(result.getSuccessCount() == 0, "There should not any cached file in consistent state");
  }

  @Test
  public void testValidatorFileVisitor_someCorruptedFiles() throws TException, IOException
  {
    String filePath = CacheUtil.getLocalPath(TEST_REMOTE_PATH_ONE, conf);
    Files.createFile(Paths.get(filePath));
    log.info("FilePath " + filePath);

    filePath = CacheUtil.getLocalPath(TEST_REMOTE_PATH_TWO, conf);
    Files.createFile(Paths.get(filePath));
    log.info("FilePath " + filePath);

    CacheStatusRequest request = new CacheStatusRequest(TEST_REMOTE_PATH_ONE, TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
        TEST_START_BLOCK, TEST_END_BLOCK, ClusterType.TEST_CLUSTER_MANAGER.ordinal());
    bookKeeper.getCacheStatus(request);

    UpdateCacheRequest updateRequest = new UpdateCacheRequest(TEST_REMOTE_PATH_ONE, TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
        TEST_START_BLOCK, TEST_END_BLOCK, 300);
    bookKeeper.setAllCached(updateRequest);

    request = new CacheStatusRequest(TEST_REMOTE_PATH_TWO, TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
        TEST_START_BLOCK, TEST_END_BLOCK, ClusterType.TEST_CLUSTER_MANAGER.ordinal());
    bookKeeper.getCacheStatus(request);

    updateRequest = new UpdateCacheRequest(TEST_REMOTE_PATH_TWO, TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
        TEST_START_BLOCK, TEST_END_BLOCK, 100);
    bookKeeper.setAllCached(updateRequest);

    FileValidatorResult result = validate();

    assertTrue(result.getTotalFiles() == 2, "Total file count didn't match");
    assertTrue(result.getCorruptedCachedFiles().size() == 1, "One çached file should be in corrupted state");
    assertTrue(result.getSuccessCount() == 1, "One cached file should be in consistent state");
  }

  /**
   * Run and verify a {@link FileValidatorVisitor}
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
    FileValidatorResult result = validate();

    verifyCorrectness(fileGenResult, result);
  }

  /**
   * Run a {@link FileValidatorVisitor} on all configured cache directories.
   *
   * @return The result of the validation.
   * @throws IOException if an I/O error occurs while visiting files.
   */
  private FileValidatorResult validate() throws IOException
  {
    final FileValidatorVisitor validator = new FileValidatorVisitor(conf, bookKeeper);

    final Map<Integer, String> diskMap = CacheUtil.getCacheDiskPathsMap(conf);
    for (int disk = 0; disk < diskMap.size(); disk++) {
      Files.walkFileTree(Paths.get(diskMap.get(disk)), validator);
    }

    return validator.getResult();
  }

  /**
   * Verify the correctness of the {@link FileValidatorVisitor}
   *
   * @param fileGenResult     The fileValidatorResult of the test file generation.
   * @param fileValidatorResult  The fileValidatorResult of the file validation.
   */
  private void verifyCorrectness(ValidatorFileGen.FileGenResult fileGenResult, FileValidatorResult fileValidatorResult)
  {
    assertEquals(fileValidatorResult.getTotalFiles(), fileGenResult.getTotalCacheFilesCreated());
    assertEquals(fileValidatorResult.getFilesWithoutMD(), fileGenResult.getFilesWithoutMd());
  }
}
