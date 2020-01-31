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

import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class CacheUtil
{
  private static final Log log = LogFactory.getLog(CacheUtil.class.getName());

  private CacheUtil()
  {
  }

  /**
   * Create the necessary directories for caching file data.
   *
   * @param conf  The current Hadoop configuration.
   * @throws FileNotFoundException if the parent directory for the cache cannot be found.
   */
  public static void createCacheDirectories(Configuration conf) throws FileNotFoundException
  {
    final List<String> dirPrefixList = getDirPrefixList(conf);
    final int maxDisks = CacheConfig.getCacheMaxDisks(conf);
    boolean parentDirectoryExists = false;

    for (String dirPrefix : dirPrefixList) {
      for (int i = 0; i < maxDisks; ++i) {
        final String cacheParentDir = dirPrefix + i;

        if (exists(cacheParentDir)) {
          parentDirectoryExists = true;
          final String cacheDirPath = cacheParentDir + "/" + CacheConfig.getCacheDataDirSuffix(conf);
          createCacheDirectory(cacheDirPath, conf);
        }
      }
    }

    // If count based directories dont exist then fallback to prefix+suffix based directories
    if (!parentDirectoryExists) {
      for (String dirPrefix : dirPrefixList) {
        if (exists(dirPrefix)) {
          parentDirectoryExists = true;
          final String cacheDirPath = dirPrefix + "/" + CacheConfig.getCacheDataDirSuffix(conf);
          createCacheDirectory(cacheDirPath, conf);
        }
      }
    }

    if (!parentDirectoryExists) {
      throw new FileNotFoundException(String.format("None of the cache parent directories exists"));
    }
  }

  /**
   * Get the number of disks available for use on the filesystem.
   *
   * @param conf  The current Hadoop configuration.
   * @return The number of disks available.
   */
  public static int getCacheDiskCount(Configuration conf)
  {
    return getCacheDiskPathsMap(conf).size();
  }

  /**
   * Get a map of disk numbers to their corresponding paths on the file system.
   *
   * @param conf  The current Hadoop configuration.
   * @return A map of disk numbers to disk paths.
   */
  public static HashMap<Integer, String> getCacheDiskPathsMap(final Configuration conf)
  {
    Supplier<HashMap<Integer, String>> diskPathMapSupplier = Suppliers.memoize(new Supplier<HashMap<Integer, String>>()
    {
      @Override
      public HashMap<Integer, String> get()
      {
        final HashMap<Integer, String> dirPathMap = new HashMap<>();
        final List<String> dirPrefixList = getDirPrefixList(conf);
        final String dirSuffix = CacheConfig.getCacheDataDirSuffix(conf);
        final int maxDisks = CacheConfig.getCacheMaxDisks(conf);

        int numDisks = 0;
        for (String dirPrefix : dirPrefixList) {
          for (int i = 0; i < maxDisks; ++i) {
            final String cacheDirPath = dirPrefix + i + "/" + dirSuffix;
            if (exists(cacheDirPath)) {
              dirPathMap.put(numDisks, dirPrefix + i);
              ++numDisks;
            }
          }
        }

        if (numDisks == 0) {
          for (String dirPrefix : dirPrefixList) {
            final String cacheDirPath = dirPrefix + "/" + dirSuffix;
            if (exists(cacheDirPath)) {
              dirPathMap.put(numDisks, dirPrefix);
              ++numDisks;
            }
          }
        }

        return dirPathMap;
      }
    });
    return diskPathMapSupplier.get();
  }

  /**
   * Get the path for the provided disk number.
   *
   * @param dirIndex  The index of the disk directory to fetch.
   * @param conf      The current Hadoop configuration.
   * @return The path of the desired cache disk.
   */
  public static String getDirPath(int dirIndex, Configuration conf)
  {
    final HashMap<Integer, String> dirPrefixMap = getCacheDiskPathsMap(conf);
    return dirPrefixMap.get(dirIndex);
  }

  /**
   * Determine the local path for a given remote path.
   *
   * @param remotePath  The path for a remote location.
   * @param conf        The current Hadoop configuration.
   * @return The local path location.
   */
  public static String getLocalPath(String remotePath, Configuration conf)
  {
    final String absLocation = getDirectory(remotePath, conf);
    return absLocation + "/" + getName(remotePath);
  }

  public static String getRemotePath(String localPath, Configuration conf)
  {
    String cacheSuffix = CacheConfig.getCacheDataDirSuffix(conf);
    int index = localPath.indexOf(cacheSuffix);
    String remotePath = localPath.substring(index + cacheSuffix.length() - 1);
    return remotePath;
  }

  /**
   * Determine the metadata file path for a given remote path.
   *
   * @param remotePath  The path for a remote location.
   * @param conf        The current Hadoop configuration.
   * @return The metadata file path.
   */
  public static String getMetadataFilePath(String remotePath, Configuration conf)
  {
    final String absLocation = getDirectory(remotePath, conf);
    return absLocation + "/" + getName(remotePath) + CacheConfig.getCacheMetadataFileSuffix(conf);
  }

  /**
   * Determine whether the file at the given path is a metadata file.
   *
   * @param filePath  The path to a possible metadata file.
   * @param conf      The current Hadoop configuration.
   * @return true if the file is a metadata file, false otherwise
   */
  public static boolean isMetadataFile(String filePath, Configuration conf)
  {
    return filePath.endsWith(CacheConfig.getCacheMetadataFileSuffix(conf));
  }

  /**
   * Determine whether data should be cached.
   *
   * @param path  The data location path.
   * @param conf  The current Hadoop configuration.
   * @return True if caching should be skipped, false otherwise.
   */
  public static boolean skipCache(String path, Configuration conf)
  {
    if (!CacheConfig.isCacheDataEnabled(conf)) {
      return true;
    }

    if (!isLocationAllowedToCache(path, conf)) {
      return true;
    }

    if (!isTableAllowedToCache(conf)) {
      return true;
    }

    if (!minColumnsSelected(conf)) {
      return true;
    }

    return false;
  }

  /**
   * Create the cache directory from the provided path.
   *
   * @param cacheDirPath  The path for which to create the directory.
   */
  private static void createCacheDirectory(String cacheDirPath, Configuration conf)
  {
    final File cacheDir = new File(cacheDirPath);
    cacheDir.mkdirs();
    File parentFile = cacheDir;
    do {
      parentFile.setWritable(true, false);
      parentFile = parentFile.getParentFile();
    } while (parentFile.getAbsolutePath().contains(CacheConfig.getCacheDataDirSuffix(conf).split("/")[1]));
  }

  /**
   * Determines whether a given file name exists on the filesystem.
   *
   * @param filePath  The file path to check.
   * @return True if the file exists, false otherwise.
   */
  private static boolean exists(String filePath)
  {
    return (new File(filePath)).exists();
  }

  /**
   * Get the cache directory path for a given remote path.
   *
   * @param remotePath  The remote path location
   * @param conf        The current Hadoop configuration.
   * @return The path to the cache directory.
   */
  private static String getDirectory(String remotePath, Configuration conf)
  {
    final String parentPath = getParent(remotePath);
    String relLocation = parentPath;

    if (parentPath.contains(":")) {
      URI parentUri = new Path(parentPath).toUri();
      StringBuilder sb = new StringBuilder();
      sb.append(parentUri.getAuthority() != null ? parentUri.getAuthority() : "");
      sb.append(parentUri.getPath() != null ? parentUri.getPath() : "");
      relLocation = sb.toString();
      if (relLocation.startsWith("/")) {
        relLocation = relLocation.substring(1);
      }
    }

    final String absLocation = getLocalDirFor(remotePath, conf) + relLocation;
    createCacheDirectory(absLocation, conf);

    return absLocation;
  }

  /**
   * Get the list of cache directory prefixes.
   *
   * @param conf  The current Hadoop configuration.
   * @return A list containing all provided prefixes,
   */
  public static List<String> getDirPrefixList(Configuration conf)
  {
    final String cacheDirPrefixList = CacheConfig.getCacheDirPrefixList(conf);
    return Arrays.asList(cacheDirPrefixList.split("\\s*,\\s*"));
  }

  /**
   * Get the local directory path for a given remote path.
   *
   * @param remotePath  The remote path location.
   * @param conf        The current Hadoop configuration.
   * @return The local directory path.
   */
  private static String getLocalDirFor(String remotePath, Configuration conf)
  {
    final int numDisks = getCacheDiskCount(conf);
    final int numBuckets = 100 * numDisks;
    final HashFunction hf = Hashing.murmur3_32();
    final HashCode hc = hf.hashString(remotePath, Charsets.UTF_8);
    final int bucket = Math.abs(hc.asInt()) % numBuckets;
    final int dirNum = (bucket / numDisks) % numDisks;

    final String dirname = getDirPath(dirNum, conf) + CacheConfig.getCacheDataDirSuffix(conf);
    return dirname;
  }

  /**
   * Get the directory name for a given remote path.
   *
   * @param remotePath  The remote path to parse.
   * @return The remote path's directory name.
   */
  public static String getName(String remotePath)
  {
    return remotePath.contains("/") ? remotePath.substring(remotePath.lastIndexOf('/') + 1) : remotePath;
  }

  /**
   * Get the parent directory for a given remote path.
   *
   * @param remotePath  The remote path to parse.
   * @return The remote path's parent directory.
   */
  private static String getParent(String remotePath)
  {
    return remotePath.contains("/") ? remotePath.substring(0, remotePath.lastIndexOf('/')) : "";
  }

  /**
   * Determines if the given path is allowed to be cached.
   *
   * @param path  The path to cache.
   * @param conf  The current Hadoop configuration.
   * @return True if the location can be cached, false otherwise.
   */
  private static boolean isLocationAllowedToCache(String path, Configuration conf)
  {
    // Check whitelist first, if location matches both whitelist and blacklist, blacklist it
    final String whitelist = CacheConfig.getCacheDataLocationWhitelist(conf);
    if (whitelist.length() > 0 && !path.matches(whitelist)) {
      return false;
    }

    final String blacklist = CacheConfig.getCacheDataLocationBlacklist(conf);
    if (blacklist.length() > 0 && path.matches(blacklist)) {
      return false;
    }

    return true;
  }

  /**
   * Determines whether a table is allowed to be cached.
   *
   * @param conf  The current Hadoop configuration.
   * @return True if the table can be cached, false otherwise.
   */
  private static boolean isTableAllowedToCache(Configuration conf)
  {
    final String table = CacheConfig.getCacheDataTable(conf);
    if (table == null || table.isEmpty()) {
      // Support not added by engine
      return true;
    }

    final String whitelist = CacheConfig.getCacheDataTableWhitelist(conf);
    if (whitelist.length() > 0 && !table.matches(whitelist)) {
      return false;
    }

    return true;
  }

  /**
   * Determines whether the minimum number of columns has been selected.
   *
   * @param conf  The current Hadoop configuration
   * @return True if the minimum cumber of columns was chosen, false otherwise.
   */
  private static boolean minColumnsSelected(Configuration conf)
  {
    return CacheConfig.getCacheDataMinColumns(conf) <= CacheConfig.getCacheDataChosenColumns(conf);
  }
}
