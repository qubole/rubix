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
package com.qubole.rubix.spi;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestCacheUtil
{
  private static final Log log = LogFactory.getLog(CacheUtil.class);
  private static final String cacheTestDirPrefix = System.getProperty("java.io.tmpdir") + "/cacheUtilTest/";
  private static final int maxDisks = 5;

  private final Configuration conf = new Configuration();

  @BeforeClass
  public void initializeCacheDirectories() throws IOException
  {
    Set<PosixFilePermission> perms =
            PosixFilePermissions.fromString("rwx------");
    FileAttribute<Set<PosixFilePermission>> attr =
            PosixFilePermissions.asFileAttribute(perms);
    Files.createDirectories(Paths.get(cacheTestDirPrefix), attr);
    for (int i = 0; i < maxDisks; i++) {
      Files.createDirectories(Paths.get(cacheTestDirPrefix, String.valueOf(i)));
    }
  }

  @AfterClass
  public void tearDownClass() throws IOException
  {
    cleanCacheDirectories(cacheTestDirPrefix);
  }

  @BeforeMethod
  public void setUpConfiguration()
  {
    conf.clear();
    CacheConfig.setpathEncryptionEnabled(conf,true);
  }

  private static String getHashedPath(String relLocation)
  {
    String hashRelLocation = relLocation;
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] pathBytes = md.digest(relLocation.getBytes());
      StringBuilder sb = new StringBuilder();
      for(int i=0; i < pathBytes.length; i++) {
        sb.append(Integer.toString((pathBytes[i] & 0xff) + 0x100, 16).substring(1));
      }
      hashRelLocation = sb.toString();
    }
    catch (NoSuchAlgorithmException e)
    {
      log.error("No Such Algorithm for Hashing " + e.toString());
    }
    return hashRelLocation;
  }

  @Test
  public void testCreateCacheDirectories_noCacheParentExists()
  {
    String cacheDataDirs = cacheTestDirPrefix + "doesNotExist1/," + cacheTestDirPrefix + "doesNotExist2/";
    CacheConfig.setCacheDataDirPrefix(conf, cacheDataDirs);
    CacheConfig.setCacheDataDirSuffix(conf, "/fcache/");
    CacheConfig.setMaxDisks(conf, maxDisks);

    try {
      CacheUtil.createCacheDirectories(conf);
    }
    catch (FileNotFoundException e) {
      assertEquals(e.getMessage(), "None of the cache parent directories exists");
      return;
    }

    fail("Cache directory creation should not succeed.");
  }

  @Test
  public void testCreateCacheDirectories_oneCacheParentExists()
  {
    CacheConfig.setCacheDataDirPrefix(conf, cacheTestDirPrefix);
    CacheConfig.setCacheDataDirSuffix(conf, "/fcache/");
    CacheConfig.setMaxDisks(conf, maxDisks + 1);

    try {
      CacheUtil.createCacheDirectories(conf);
    }
    catch (FileNotFoundException e) {
      fail("One of the cache parent directories exists");
    }
  }

  @Test
  public void testGetCacheDiskCount()
  {
    CacheConfig.setCacheDataDirPrefix(conf, cacheTestDirPrefix);
    CacheConfig.setCacheDataDirSuffix(conf, "/fcache/");
    CacheConfig.setMaxDisks(conf, maxDisks);

    createCacheDirectoriesForTest(conf);

    int diskCount = CacheUtil.getCacheDiskCount(conf);
    assertEquals(diskCount, maxDisks, "Sizes don't match!");
  }

  @Test
  public void testGetDiskPathsMap()
  {
    CacheConfig.setCacheDataDirPrefix(conf, cacheTestDirPrefix);
    CacheConfig.setCacheDataDirSuffix(conf, "/fcache/");
    CacheConfig.setMaxDisks(conf, maxDisks);

    createCacheDirectoriesForTest(conf);

    HashMap<Integer, String> diskPathsMap = CacheUtil.getCacheDiskPathsMap(conf);
    assertEquals(diskPathsMap.get(0), cacheTestDirPrefix + "0", "Sizes don't match!");
  }

  @Test
  public void testGetDirPath()
  {
    CacheConfig.setCacheDataDirPrefix(conf, cacheTestDirPrefix);
    CacheConfig.setCacheDataDirSuffix(conf, "/fcache/");
    CacheConfig.setMaxDisks(conf, 2);

    createCacheDirectoriesForTest(conf);
    String dirPath = CacheUtil.getDirPath(1, conf);

    assertEquals(dirPath, cacheTestDirPrefix + "1", "Paths don't match");
  }

  @Test
  public void testGetLocalPath()
  {
    String localRelPath = "testbucket/123/4566/789";
    String remotePath = "s3://" + localRelPath;
    CacheConfig.setCacheDataDirPrefix(conf, cacheTestDirPrefix);
    CacheConfig.setCacheDataDirSuffix(conf, "/fcache/");
    CacheConfig.setMaxDisks(conf, 1);

    createCacheDirectoriesForTest(conf);
    //String absLocation = CacheUtil.getDirectory(remotePath, conf);
    String localPath = CacheUtil.getLocalPath(localRelPath, conf);
    //assertEquals(localPath, cacheTestDirPrefix + "0" + "/fcache/" + getHashedPath( cacheTestDirPrefix + "0" + "/fcache/" + remotePath), "Paths not equal!");
    assertEquals( localPath, cacheTestDirPrefix + "0" + "/fcache/" + getHashedPath("testbucket/123/4566") + "/789", "Paths not equal!");
  }

  @Test
  public void testGetLocalPath_noRemotePathScheme()
  {
    String localRelPath = "testbucket/123/4566/789";
    CacheConfig.setCacheDataDirPrefix(conf, cacheTestDirPrefix);
    CacheConfig.setCacheDataDirSuffix(conf, "/fcache/");
    CacheConfig.setMaxDisks(conf, 1);

    createCacheDirectoriesForTest(conf);

    String localPath = CacheUtil.getLocalPath(localRelPath, conf);
    //assertEquals(localPath, cacheTestDirPrefix + "0" + "/fcache/" + localRelPath, "Paths not equal!");
    assertEquals(localPath, cacheTestDirPrefix + "0" + "/fcache/" + getHashedPath("testbucket/123/4566") + "/789", "Paths not equal!");
  }

  @Test
  public void testGetLocalPath_singleLevel()
  {
    String localRelPath = "testbucket";
    CacheConfig.setCacheDataDirPrefix(conf, cacheTestDirPrefix);
    CacheConfig.setCacheDataDirSuffix(conf, "/fcache/");
    CacheConfig.setMaxDisks(conf, 1);

    createCacheDirectoriesForTest(conf);

    String localPath = CacheUtil.getLocalPath(localRelPath, conf);
    assertEquals(localPath, cacheTestDirPrefix + "0" + "/fcache/" + getHashedPath("") + "/testbucket", "Paths not equal!");
  }

  @Test
  public void testGetMetadataFilePathForS3() throws IOException
  {
    String localRelPath = "testbucket/123/456/789";
    String remotePath = "s3://" + localRelPath;
    CacheConfig.setCacheDataDirPrefix(conf, cacheTestDirPrefix);
    CacheConfig.setCacheDataDirSuffix(conf, "/fcache/");
    CacheConfig.setMaxDisks(conf, 1);

    createCacheDirectoriesForTest(conf);

    String localPath = CacheUtil.getMetadataFilePath(remotePath, conf);
    assertEquals(localPath, cacheTestDirPrefix + "0" + "/fcache/" + getHashedPath("testbucket/123/456") + "/789_mdfile", "Paths not equal!");

    localRelPath = "123";
    remotePath = "s3://" + localRelPath;
    localPath = CacheUtil.getMetadataFilePath(remotePath, conf);
    assertEquals(localPath, cacheTestDirPrefix + "0" + "/fcache/" + getHashedPath("") + "/123_mdfile", "Paths not equal!");
  }

  @Test
  public void testGetMetadataFilePathForLocalFileSystem() throws IOException
  {
    String localRelPath = "testbucket/123/456/789";
    String remotePath = "file:///" + localRelPath;
    CacheConfig.setCacheDataDirPrefix(conf, cacheTestDirPrefix);
    CacheConfig.setCacheDataDirSuffix(conf, "/fcache/");
    CacheConfig.setMaxDisks(conf, 1);

    createCacheDirectoriesForTest(conf);

    String localPath = CacheUtil.getMetadataFilePath(remotePath, conf);
    assertEquals(localPath, cacheTestDirPrefix + "0" + "/fcache/" + getHashedPath("testbucket/123/456") + "/789_mdfile", "Paths not equal!");
  }

  @Test
  public void testGetMetadataFilePathForWasb() throws IOException
  {
    String localRelPath = "testbucket/123/456/789";
    String remotePath = "wasb://" + localRelPath;
    CacheConfig.setCacheDataDirPrefix(conf, cacheTestDirPrefix);
    CacheConfig.setCacheDataDirSuffix(conf, "/fcache/");
    CacheConfig.setMaxDisks(conf, 1);

    createCacheDirectoriesForTest(conf);

    String localPath = CacheUtil.getMetadataFilePath(remotePath, conf);
    assertEquals(localPath, cacheTestDirPrefix + "0" + "/fcache/" + getHashedPath("testbucket/123/456") + "/789_mdfile", "Paths not equal!");
  }

  @Test
  public void testSkipCache_cachingDisabled()
  {
    CacheConfig.setCacheDataEnabled(conf, false);
    String testPath = "/test/path";

    boolean skipCache = CacheUtil.skipCache(testPath, conf);

    assertTrue(skipCache, "Cache is not being skipped!");
  }

  @Test
  public void testSkipCache_LocationNotOnWhitelist()
  {
    CacheConfig.setCacheDataEnabled(conf, true);
    CacheConfig.setCacheDataLocationWhitelist(conf, "^((?!test).)*$");
    String testPath = "/test/path";

    boolean skipCache = CacheUtil.skipCache(testPath, conf);

    assertTrue(skipCache, "Cache is not being skipped!");
  }

  @Test
  public void testSkipCache_LocationOnBlackList()
  {
    CacheConfig.setCacheDataEnabled(conf, true);
    CacheConfig.setCacheDataLocationBlacklist(conf, ".*test.*");
    String testPath = "/test/path";

    boolean skipCache = CacheUtil.skipCache(testPath, conf);

    assertTrue(skipCache, "Cache is not being skipped!");
  }

  @Test
  public void testSkipCache_LocationOnWhitelistAndBlackList()
  {
    CacheConfig.setCacheDataEnabled(conf, true);
    CacheConfig.setCacheDataLocationWhitelist(conf, ".*test.*");
    CacheConfig.setCacheDataLocationBlacklist(conf, ".*test.*");
    String testPath = "/test/path";

    boolean skipCache = CacheUtil.skipCache(testPath, conf);

    assertTrue(skipCache, "Cache is not being skipped!");
  }

  @Test
  public void testSkipCache_tableNotAllowed()
  {
    CacheConfig.setCacheDataEnabled(conf, true);
    CacheConfig.setCacheDataLocationWhitelist(conf, ".*test.*");
    CacheConfig.setCacheDataTable(conf, "testTable");
    CacheConfig.setCacheDataTableWhitelist(conf, "^((?!testTable).)*$");
    String testPath = "/test/path";

    boolean skipCache = CacheUtil.skipCache(testPath, conf);

    assertTrue(skipCache, "Cache is not being skipped!");
  }

  @Test
  public void testSkipCache_noTableName()
  {
    CacheConfig.setCacheDataEnabled(conf, true);
    String testPath = "/test/path";

    boolean skipCache = CacheUtil.skipCache(testPath, conf);

    assertFalse(skipCache, "Cache is being skipped!");
  }

  @Test
  public void testSkipCache_minColsGreaterThanChosenColumns()
  {
    CacheConfig.setCacheDataEnabled(conf, true);
    CacheConfig.setCacheDataLocationWhitelist(conf, ".*test.*");
    CacheConfig.setCacheDataTable(conf, "testTable");
    CacheConfig.setCacheDataTableWhitelist(conf, ".*testTable.*");
    CacheConfig.setCacheDataMinColumns(conf, 5);
    CacheConfig.setCacheDataChosenColumns(conf, 3);
    String testPath = "/test/path";

    boolean skipCache = CacheUtil.skipCache(testPath, conf);

    assertTrue(skipCache, "Cache is not being skipped!");
  }

  @Test
  public void testSkipCache_cacheNotSkipped()
  {
    CacheConfig.setCacheDataEnabled(conf, true);
    CacheConfig.setCacheDataLocationWhitelist(conf, ".*test.*");
    CacheConfig.setCacheDataTable(conf, "testTable");
    CacheConfig.setCacheDataTableWhitelist(conf, ".*testTable.*");
    CacheConfig.setCacheDataMinColumns(conf, 3);
    CacheConfig.setCacheDataChosenColumns(conf, 5);
    String testPath = "/test/path";

    boolean skipCache = CacheUtil.skipCache(testPath, conf);

    assertFalse(skipCache, "Cache is being skipped!");
  }

  /**
   * Create the cache directories necessary for running the test.
   *
   * @param conf  The current Hadoop configuration.
   */
  private void createCacheDirectoriesForTest(Configuration conf)
  {
    try {
      CacheUtil.createCacheDirectories(conf);
    }
    catch (FileNotFoundException e) {
      fail("Could not create cache directories: " + e.getMessage());
    }
  }

  private void cleanCacheDirectories(String rootDirPath) throws IOException
  {
    Files.walkFileTree(Paths.get(rootDirPath), new CacheCleanFileVisitor());
    Files.deleteIfExists(Paths.get(rootDirPath));
  }

  public static class CacheCleanFileVisitor extends SimpleFileVisitor<Path>
  {
    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
    {
      Files.delete(file);
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException
    {
      Files.delete(dir);
      return FileVisitResult.CONTINUE;
    }
  }
}
