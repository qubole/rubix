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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by stagra on 14/2/16.
 */

/*
 * The conf provided the CachingFileSystem should have appropriate configs set if user wish to override defaults
 * Different engines provides different ways to do this:
 * Presto: "hive.config.resources" can point to files storing hadoop configurations
 * Hive: --hiveconf argument can be used
 * Spark: --config spark.hadoop.XYZ can be used
 */

public class CacheConfig
{
  public static final String RUBIX_SCHEME = "rubix";
  public static final String DATA_CACHE_ENABLED = "hadoop.cache.data.enabled";
  public static final String DATA_CACHE_TABLE_WHITELIST = "hadoop.cache.data.table.whitelist";
  public static final String DATA_CACHE_TABLE = "hadoop.cache.data.table";
  // Internal
  // In strict mode, queries will error out if BookKeeper cannot be reached
  public static final String DATA_CACHE_STRICT_MODE = "hadoop.cache.data.strict.mode";
  public static final String DATA_CACHE_LOCATION_WHITELIST = "hadoop.cache.data.location.whitelist"; // only these locations will cached
  public static final String DATA_CACHE_LOCATION_BLACKLIST = "hadoop.cache.data.location.blacklist"; // these locations will be skipped, takes priority over Whitelist
  public static final String DATA_CACHE_TABLE_MIN_COLS = "hadoop.cache.data.table.columns.min";
  public static final String DATA_CACHE_TABLE_COLS_CHOSEN = "hadoop.cache.data.table.columns.chosen";

  public static String dataCacheExpirationConf = "hadoop.cache.data.expiration";
  public static String dataCacheExpirationAfterWriteConf = "hadoop.cache.data.expiration.after-write";
  public static String dataCacheFullnessConf = "hadoop.cache.data.fullness.percentage";
  public static String dataCacheDirprefixesConf = "hadoop.cache.data.dirprefix.list";
  public static String blockSizeConf = "hadoop.cache.data.block-size";
  public static String dataCacheBookkeeperPortConf = "hadoop.cache.data.bookkeeper.port";
  public static String dataCacheBookkeeperMaxThreadsConf = "hadoop.cache.data.bookkeeper.max-threads";
  private static String clientTimeoutConf = "hadoop.cache.data.client.timeout";
  private static String maxRetriesConf = "hadoop.cache.data.client.num-retries";
  private static String localTransferBufferSizeConf = "hadoop.cache.data.buffer.size";
  public static String localServerPortConf = "hadoop.cache.data.local.server.port";
  private static String dataMaxHeaderSizeConf = "hadoop.cache.data.transfer.header.size";
  private static String diskReadBufferSizeConf = "hadoop.cache.data.disk.read.buffer.size";
  public static String socketReadTimeOutConf = "hadoop.cache.network.socket.read.timeout";
  private static String diskMonitorIntervalConf = "hadoop.cache.disk.monitor.interval";

  public static String parallelWarmupEnable = "rubix.parallel.warmup";
  public static String processThreadInitalDelay = "rubix.request.process.inital.delay";
  public static String processThreadInterval = "rubix.request.process.interval";
  public static String remoteFetchProcessInterval = "rubix.remotefetch.interval";
  public static String numRemoteFetchThreads = "rubix.remotefetch.threads";

  static String fileCacheDirSuffixConf = "/fcache/";
  static int maxDisksConf = 5;

  // default values
  private static final int dataCacheExpiry = Integer.MAX_VALUE;
  // Keepnig this low to workaround the Guava Cache static weighing limitation
  private static final int dataCacheExpiryAfterWrite = Integer.MAX_VALUE; // In seconds, inifinite by default
  private static final int dataCacheFullness = 80; // percent
  private static final String dataCacheDirPrefixes = "/media/ephemeral";
  private static final int blockSize = 1 * 1024 * 1024; // 1MB
  private static int serverPort = 8899;
  private static int localServerPort = 8898;
  private static int serverMaxThreads = Integer.MAX_VALUE;
  public static int localTransferbufferSize = 10 * 1024 * 1024;
  public static final int diskReadBufferSizeDefault = 1024;
  public static int socketReadTimeOutDefault = 30000; // In milliseconds.
  private static int diskMonitorInterval = 10; // in seconds

  private static int processThreadInitalDelayDefault = 1000;
  private static int processThreadIntervalDefault = 1000;
  private static int remoteFetchProcessIntervalDefault = 10000;
  private static int numRemoteFetchThreadsDefault = 10;

  private static final Log log = LogFactory.getLog(CacheConfig.class.getName());

  private CacheConfig()
  {
  }

  public static int getSocketReadTimeOutDefault(Configuration conf)
  {
    return conf.getInt(socketReadTimeOutConf, socketReadTimeOutDefault);
  }

  public static int getDiskReadBufferSizeDefault(Configuration conf)
  {
    return conf.getInt(diskReadBufferSizeConf, diskReadBufferSizeDefault);
  }

  public static int getCacheDataExpiration(Configuration conf)
  {
    return conf.getInt(dataCacheExpirationConf, dataCacheExpiry);
  }

  public static int getCacheDataExpirationAfterWrite(Configuration conf)
  {
    return conf.getInt(dataCacheExpirationAfterWriteConf, dataCacheExpiryAfterWrite);
  }

  public static int getCacheDataFullnessPercentage(Configuration conf)
  {
    return conf.getInt(dataCacheFullnessConf, dataCacheFullness);
  }

  public static String getCacheDirPrefixList(Configuration conf)
  {
    return conf.get(dataCacheDirprefixesConf, dataCacheDirPrefixes);
  }

  public static int getBlockSize(Configuration conf)
  {
    return conf.getInt(blockSizeConf, blockSize);
  }

  public static int getServerPort(Configuration conf)
  {
    return conf.getInt(dataCacheBookkeeperPortConf, serverPort);
  }

  public static int getServerMaxThreads(Configuration conf)
  {
    return conf.getInt(dataCacheBookkeeperMaxThreadsConf, serverMaxThreads);
  }

  public static int getClientTimeout(Configuration conf)
  {
    return conf.getInt(clientTimeoutConf, 10000); //ms
  }

  public static int getMaxRetries(Configuration conf)
  {
    return conf.getInt(maxRetriesConf, 3);
  }

  public static int numDisks(Configuration conf)
  {
    return getDiskPathsMap(conf).size();
  }

  public static HashMap<Integer, String> getDiskPathsMap(final Configuration conf)
  {
    Supplier<HashMap<Integer, String>> s = Suppliers.memoize(new Supplier<HashMap<Integer, String>>()
    {
      @Override
      public HashMap<Integer, String> get()
      {
        HashMap<Integer, String> dirPathMap = new HashMap<>();
        int ndisks = 0;
        List<String> dirPrefixList = getDirPrefixList(conf);
        for (String dirPrefix : dirPrefixList) {
          for (int i = 0; i < maxDisksConf; ++i) {
            log.debug("Checking " + dirPrefix + i);
            if (exists(dirPrefix + i)) {
              File dir = new File(dirPrefix + i + fileCacheDirSuffixConf);
              dir.mkdir();
              dir.setWritable(true, false);
              dirPathMap.put(ndisks, dirPrefix + i);
              ++ndisks;
            }
          }
        }
        return dirPathMap;
      }
    });
    return s.get();
  }

  private static List<String> getDirPrefixList(Configuration conf)
  {
    String cacheDirPrefixList = getCacheDirPrefixList(conf);
    return Arrays.asList(cacheDirPrefixList.split("\\s*,\\s*"));
  }

  public static String getDirPath(Configuration conf, int d)
  {
    HashMap<Integer, String> dirPrefixMap = getDiskPathsMap(conf);
    return dirPrefixMap.get(d);
  }

  private static boolean exists(String filename)
  {
    return (new File(filename)).exists();
  }

  public static String getLocalPath(String remotePath, Configuration conf)
  {
    String absLocation = getDirectory(remotePath, conf);
    return absLocation + "/" + getName(remotePath);
  }

  public static String getMDFile(String remotePath, Configuration conf)
  {
    String absLocation = getDirectory(remotePath, conf);
    return absLocation + "/" + getName(remotePath) + "_mdfile";
  }

  public static String getDirectory(String remotePath, Configuration conf)
  {
    String parentPath = getParent(remotePath);
    String relLocation = parentPath.indexOf(':') == -1 ? parentPath : parentPath.substring(parentPath.indexOf(':') + 3);
    String absLocation = getLocalDirFor(remotePath, conf) + relLocation;
    File parent = new File(absLocation);
    parent.mkdirs();
    parent.setWritable(true, false);
    return absLocation;
  }

  public static String getLocalDirFor(String remotePath, Configuration conf)
  {
    int numDisks = numDisks(conf);
    int numBuckets = 100 * numDisks;
    HashFunction hf = Hashing.murmur3_32();
    HashCode hc = hf.hashString(remotePath, Charsets.UTF_8);
    int bucket = Math.abs(hc.asInt()) % numBuckets;
    int dirNum = (bucket / numDisks) % numDisks;

    String dirname = getDirPath(conf, dirNum) + CacheConfig.fileCacheDirSuffixConf;
    return dirname;
  }

  public static String getName(String remotePath)
  {
    return remotePath.lastIndexOf('/') == -1 ? remotePath : remotePath.substring(remotePath.lastIndexOf('/'));
  }

  public static String getParent(String remotePath)
  {
    return remotePath.lastIndexOf('/') == -1 ? "" : remotePath.substring(0, remotePath.lastIndexOf('/'));
  }

  static int getCacheDataChosenColumns(Configuration c)
  {
    return c.getInt(DATA_CACHE_TABLE_COLS_CHOSEN, 0);
  }

  // Internal
  // In strict mode, queries will error out if BookKeeper cannot be reached

  public static boolean isStrictMode(Configuration c)
  {
    return c.getBoolean(DATA_CACHE_STRICT_MODE, false);
  }

  // Helper methods to get information based on configuration

  public static boolean skipCache(Path path, Configuration conf)
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

  private static boolean isTableAllowedToCache(Configuration conf)
  {
    String table = CacheConfig.getCacheDataTable(conf);
    if (table == null || (table != null && table.isEmpty())) {
      // Support not added by engine
      return true;
    }

    String whitelist = CacheConfig.getCacheDataTableWhitelist(conf);
    if (whitelist.length() > 0) {
      if (!table.matches(whitelist)) {
        return false;
      }
    }

    return true;
  }

  private static boolean isLocationAllowedToCache(Path path, Configuration conf)
  {
    // Check whitelist first, if location matches whitelist and blacklist both then blacklist it
    String whitelist = CacheConfig.getCacheDataLocationWhitelist(conf);
    if (whitelist.length() > 0) {
      if (!path.toString().matches(whitelist)) {
        return false;
      }
    }
    String blacklist = CacheConfig.getCacheDataLocationBlacklist(conf);
    if (blacklist.length() > 0) {
      if (path.toString().matches(blacklist)) {
        return false;
      }
    }

    return true;
  }

  private static boolean minColumnsSelected(Configuration conf)
  {
    if (CacheConfig.getCacheDataMinColumns(conf) > CacheConfig
        .getCacheDataChosenColumns(conf)) {
      return false;
    }
    return true;
  }

  static boolean isCacheDataEnabled(Configuration c)
  {
    return c.getBoolean(DATA_CACHE_ENABLED, true);
  }

  static String getCacheDataTableWhitelist(Configuration c)
  {
    return c.get(DATA_CACHE_TABLE_WHITELIST, ".*");
  }

  static void setCacheDataTable(Configuration configuration, String table)
  {
    configuration.set(DATA_CACHE_TABLE, table);
  }

  static String getCacheDataTable(Configuration configuration)
  {
    return configuration.get(DATA_CACHE_TABLE, "");
  }

  static String getCacheDataLocationBlacklist(Configuration configuration)
  {
    return configuration.get(DATA_CACHE_LOCATION_BLACKLIST, "");
  }

  static String getCacheDataLocationWhitelist(Configuration configuration)
  {
    return configuration.get(DATA_CACHE_LOCATION_WHITELIST, ".*");
  }

  static int getCacheDataMinColumns(Configuration c)
  {
    return c.getInt(DATA_CACHE_TABLE_MIN_COLS, 0);
  }

  static void setCacheDataChosenColumns(Configuration c, int chosen)
  {
    c.setInt(DATA_CACHE_TABLE_COLS_CHOSEN, chosen);
  }

  //localTransferbufferSize for reads in LocalTransferServer
  public static int getLocalTransferBufferSize(Configuration c)
  {
    return c.getInt(localTransferBufferSizeConf, localTransferbufferSize);
  }

  public static int getLocalServerPort(Configuration conf)
  {
    return conf.getInt(localServerPortConf, localServerPort);
  }

  public static int getMaxHeaderSize(Configuration conf)
  {
    return conf.getInt(dataMaxHeaderSizeConf, 1024);
  }

  public static int getDiskMonitorInterval(Configuration conf)
  {
    return conf.getInt(diskMonitorIntervalConf, diskMonitorInterval);
  }

  public static boolean isParallelWarmupEnabled(Configuration conf)
  {
    return conf.getBoolean(parallelWarmupEnable, false);
  }

  public static int getProcessThreadInitialDelayInMs(Configuration conf)
  {
    return conf.getInt(processThreadInitalDelay, processThreadInitalDelayDefault);
  }

  public static int getProcessThreadIntervalInMs(Configuration conf)
  {
    return conf.getInt(processThreadInterval, processThreadIntervalDefault);
  }

  public static int getRemoteFetchProcessIntervalInMS(Configuration conf)
  {
    return conf.getInt(remoteFetchProcessInterval, remoteFetchProcessIntervalDefault);
  }

  public static int getRemoteFetchNumThreads(Configuration conf)
  {
    return conf.getInt(numRemoteFetchThreads, numRemoteFetchThreadsDefault);
  }
}
