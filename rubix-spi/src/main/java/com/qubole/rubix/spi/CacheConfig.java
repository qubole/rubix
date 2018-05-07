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

import org.apache.hadoop.conf.Configuration;

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

  private static final String KEY_BLOCK_SIZE = "hadoop.cache.data.block-size";
  private static final String KEY_CACHE_ENABLED = "hadoop.cache.data.enabled";
  private static final String KEY_CLIENT_TIMEOUT = "hadoop.cache.data.client.timeout";
  private static final String KEY_DATA_CACHE_EXPIRY = "hadoop.cache.data.expiration";
  private static final String KEY_DATA_CACHE_EXPIRY_AFTER_WRITE = "hadoop.cache.data.expiration.after-write";
  private static final String KEY_DATA_CACHE_DIR_PREFIX = "hadoop.cache.data.dirprefix.list";
  private static final String KEY_DATA_CACHE_DIR_SUFFIX = "hadoop.cache.data.dirsuffix";
  private static final String KEY_DATA_CACHE_FULLNESS = "hadoop.cache.data.fullness.percentage";
  private static final String KEY_DATA_CACHE_LOCATION_BLACKLIST = "hadoop.cache.data.location.blacklist"; // these locations will be skipped, takes priority over Whitelist
  private static final String KEY_DATA_CACHE_LOCATION_WHITELIST = "hadoop.cache.data.location.whitelist"; // only these locations will cached
  private static final String KEY_DATA_CACHE_MAX_DISKS = "hadoop.cache.data.max.disks";
  private static final String KEY_DATA_CACHE_STRICT_MODE = "hadoop.cache.data.strict.mode"; // Internal; in strict mode, queries will error out if BookKeeper cannot be reached
  private static final String KEY_DATA_CACHE_TABLE = "hadoop.cache.data.table";
  private static final String KEY_DATA_CACHE_TABLE_COLS_CHOSEN = "hadoop.cache.data.table.columns.chosen";
  private static final String KEY_DATA_CACHE_TABLE_MIN_COLS = "hadoop.cache.data.table.columns.min";
  private static final String KEY_DATA_CACHE_TABLE_WHITELIST = "hadoop.cache.data.table.whitelist";
  private static final String KEY_DATA_MAX_HEADER_SIZE = "hadoop.cache.data.transfer.header.size";
  private static final String KEY_DISK_MONITOR_INTERVAL = "hadoop.cache.disk.monitor.interval";
  private static final String KEY_DISK_READ_BUFFER_SIZE = "hadoop.cache.data.disk.read.buffer.size";
  private static final String KEY_LOCAL_TRANSFER_BUFFER_SIZE = "hadoop.cache.data.buffer.size";
  private static final String KEY_LOCAL_SERVER_PORT = "hadoop.cache.data.local.server.port";
  private static final String KEY_MAX_RETRIES = "hadoop.cache.data.client.num-retries";
  private static final String KEY_PARALLEL_WARMUP = "rubix.parallel.warmup";
  private static final String KEY_PROCESS_THREAD_INITIAL_DELAY = "rubix.request.process.initial.delay";
  private static final String KEY_PROCESS_THREAD_INTERVAL = "rubix.request.process.interval";
  private static final String KEY_REMOTE_FETCH_PROCESS_INTERVAL = "rubix.remotefetch.interval";
  private static final String KEY_REMOTE_FETCH_THREADS = "rubix.remotefetch.threads";
  private static final String KEY_SERVER_PORT = "hadoop.cache.data.bookkeeper.port";
  private static final String KEY_SERVER_MAX_THREADS = "hadoop.cache.data.bookkeeper.max-threads";
  private static final String KEY_SOCKET_READ_TIMEOUT = "hadoop.cache.network.socket.read.timeout";

  // default values
  private static final int DEFAULT_BLOCK_SIZE = 1 * 1024 * 1024; // 1MB
  private static final int DEFAULT_CLIENT_TIMEOUT = 10000; // ms
  private static final String DEFAULT_DATA_CACHE_DIR_PREFIX = "/media/ephemeral";
  private static final String DEFAULT_DATA_CACHE_DIR_SUFFIX = "/fcache/";
  private static final boolean DEFAULT_DATA_CACHE_ENABLED = true;
  private static final int DEFAULT_DATA_CACHE_EXPIRY_AFTER_WRITE = Integer.MAX_VALUE; // ms; infinite by default
  private static final int DEFAULT_DATA_CACHE_EXPIRY = Integer.MAX_VALUE;
  private static final int DEFAULT_DATA_CACHE_FULLNESS = 80; // percent
  private static final String DEFAULT_DATA_CACHE_LOCATION_BLACKLIST = ""; // regex
  private static final String DEFAULT_DATA_CACHE_LOCATION_WHITELIST = ".*"; // regex
  private static final int DEFAULT_DATA_CACHE_MAX_DISKS = 5;
  private static final int DEFAULT_DATA_CACHE_MIN_COLUMNS = 0;
  private static final boolean DEFAULT_DATA_CACHE_STRICT_MODE = false;
  private static final String DEFAULT_DATA_CACHE_TABLE = "";
  private static final int DEFAULT_DATA_CACHE_TABLE_COLS_CHOSEN = 0;
  // Keeping this low to workaround the Guava Cache static weighing limitation
  private static final String DEFAULT_DATA_CACHE_TABLE_WHITELIST = ".*"; // regex
  private static final int DEFAULT_DISK_MONITOR_INTERVAL = 10000; // ms
  private static final int DEFAULT_DISK_READ_BUFFER_SIZE = 1024;
  private static final int DEFAULT_LOCAL_TRANSFER_BUFFER_SIZE = 10 * 1024 * 1024; // 10MB
  private static final int DEFAULT_LOCAL_SERVER_PORT = 8898;
  private static final int DEFAULT_MAX_BUFFER_SIZE = 1024;
  private static final int DEFAULT_MAX_RETRIES = 3;
  private static final boolean DEFAULT_PARALLEL_WARMUP = false;
  private static final int DEFAULT_PROCESS_THREAD_INITIAL_DELAY = 1000; // ms
  private static final int DEFAULT_PROCESS_THREAD_INTERVAL = 1000; // ms
  private static final int DEFAULT_REMOTE_FETCH_PROCESS_INTERVAL = 10000; // ms
  private static final int DEFAULT_REMOTE_FETCH_THREADS = 10;
  private static final int DEFAULT_SERVER_MAX_THREADS = Integer.MAX_VALUE;
  private static final int DEFAULT_SERVER_PORT = 8899;
  private static final int DEFAULT_SOCKET_READ_TIMEOUT = 30000; // ms

  private CacheConfig()
  {
  }

  public static int getBlockSize(Configuration conf)
  {
    return conf.getInt(KEY_BLOCK_SIZE, DEFAULT_BLOCK_SIZE);
  }

  public static int getCacheDataChosenColumns(Configuration conf)
  {
    return conf.getInt(KEY_DATA_CACHE_TABLE_COLS_CHOSEN, DEFAULT_DATA_CACHE_TABLE_COLS_CHOSEN);
  }

  public static String getCacheDataDirSuffix(Configuration conf)
  {
    return conf.get(KEY_DATA_CACHE_DIR_SUFFIX, DEFAULT_DATA_CACHE_DIR_SUFFIX);
  }

  public static int getCacheDataExpiration(Configuration conf)
  {
    return conf.getInt(KEY_DATA_CACHE_EXPIRY, DEFAULT_DATA_CACHE_EXPIRY);
  }

  public static int getCacheDataExpirationAfterWrite(Configuration conf)
  {
    return conf.getInt(KEY_DATA_CACHE_EXPIRY_AFTER_WRITE, DEFAULT_DATA_CACHE_EXPIRY_AFTER_WRITE);
  }

  public static int getCacheDataFullnessPercentage(Configuration conf)
  {
    return conf.getInt(KEY_DATA_CACHE_FULLNESS, DEFAULT_DATA_CACHE_FULLNESS);
  }

  public static String getCacheDataLocationBlacklist(Configuration conf)
  {
    return conf.get(KEY_DATA_CACHE_LOCATION_BLACKLIST, DEFAULT_DATA_CACHE_LOCATION_BLACKLIST);
  }

  public static String getCacheDataLocationWhitelist(Configuration conf)
  {
    return conf.get(KEY_DATA_CACHE_LOCATION_WHITELIST, DEFAULT_DATA_CACHE_LOCATION_WHITELIST);
  }

  public static int getCacheDataMinColumns(Configuration conf)
  {
    return conf.getInt(KEY_DATA_CACHE_TABLE_MIN_COLS, DEFAULT_DATA_CACHE_MIN_COLUMNS);
  }

  public static String getCacheDataTable(Configuration conf)
  {
    return conf.get(KEY_DATA_CACHE_TABLE, DEFAULT_DATA_CACHE_TABLE);
  }

  public static String getCacheDataTableWhitelist(Configuration conf)
  {
    return conf.get(KEY_DATA_CACHE_TABLE_WHITELIST, DEFAULT_DATA_CACHE_TABLE_WHITELIST);
  }

  public static String getCacheDirPrefixList(Configuration conf)
  {
    return conf.get(KEY_DATA_CACHE_DIR_PREFIX, DEFAULT_DATA_CACHE_DIR_PREFIX);
  }

  public static int getCacheMaxDisks(Configuration conf)
  {
    return conf.getInt(KEY_DATA_CACHE_MAX_DISKS, DEFAULT_DATA_CACHE_MAX_DISKS);
  }

  public static int getClientTimeout(Configuration conf)
  {
    return conf.getInt(KEY_CLIENT_TIMEOUT, DEFAULT_CLIENT_TIMEOUT);
  }

  public static int getDiskMonitorInterval(Configuration conf)
  {
    return conf.getInt(KEY_DISK_MONITOR_INTERVAL, DEFAULT_DISK_MONITOR_INTERVAL);
  }

  public static int getDiskReadBufferSize(Configuration conf)
  {
    return conf.getInt(KEY_DISK_READ_BUFFER_SIZE, DEFAULT_DISK_READ_BUFFER_SIZE);
  }

  public static int getLocalServerPort(Configuration conf)
  {
    return conf.getInt(KEY_LOCAL_SERVER_PORT, DEFAULT_LOCAL_SERVER_PORT);
  }

  public static int getLocalTransferBufferSize(Configuration conf)
  {
    return conf.getInt(KEY_LOCAL_TRANSFER_BUFFER_SIZE, DEFAULT_LOCAL_TRANSFER_BUFFER_SIZE);
  }

  public static int getMaxHeaderSize(Configuration conf)
  {
    return conf.getInt(KEY_DATA_MAX_HEADER_SIZE, DEFAULT_MAX_BUFFER_SIZE);
  }

  public static int getMaxRetries(Configuration conf)
  {
    return conf.getInt(KEY_MAX_RETRIES, DEFAULT_MAX_RETRIES);
  }

  public static int getProcessThreadInitialDelay(Configuration conf)
  {
    return conf.getInt(KEY_PROCESS_THREAD_INITIAL_DELAY, DEFAULT_PROCESS_THREAD_INITIAL_DELAY);
  }

  public static int getProcessThreadInterval(Configuration conf)
  {
    return conf.getInt(KEY_PROCESS_THREAD_INTERVAL, DEFAULT_PROCESS_THREAD_INTERVAL);
  }

  public static int getRemoteFetchProcessInterval(Configuration conf)
  {
    return conf.getInt(KEY_REMOTE_FETCH_PROCESS_INTERVAL, DEFAULT_REMOTE_FETCH_PROCESS_INTERVAL);
  }

  public static int getRemoteFetchThreads(Configuration conf)
  {
    return conf.getInt(KEY_REMOTE_FETCH_THREADS, DEFAULT_REMOTE_FETCH_THREADS);
  }

  public static int getServerMaxThreads(Configuration conf)
  {
    return conf.getInt(KEY_SERVER_MAX_THREADS, DEFAULT_SERVER_MAX_THREADS);
  }

  public static int getServerPort(Configuration conf)
  {
    return conf.getInt(KEY_SERVER_PORT, DEFAULT_SERVER_PORT);
  }

  public static int getSocketReadTimeOut(Configuration conf)
  {
    return conf.getInt(KEY_SOCKET_READ_TIMEOUT, DEFAULT_SOCKET_READ_TIMEOUT);
  }

  public static boolean isCacheDataEnabled(Configuration conf)
  {
    return conf.getBoolean(KEY_CACHE_ENABLED, DEFAULT_DATA_CACHE_ENABLED);
  }

  public static boolean isStrictMode(Configuration conf)
  {
    return conf.getBoolean(KEY_DATA_CACHE_STRICT_MODE, DEFAULT_DATA_CACHE_STRICT_MODE);
  }

  public static boolean isParallelWarmupEnabled(Configuration conf)
  {
    return conf.getBoolean(KEY_PARALLEL_WARMUP, DEFAULT_PARALLEL_WARMUP);
  }

  public static void setBlockSize(Configuration conf, int blockSize)
  {
    conf.setInt(KEY_BLOCK_SIZE, blockSize);
  }

  public static void setCacheDataChosenColumns(Configuration conf, int chosenColumns)
  {
    conf.setInt(KEY_DATA_CACHE_TABLE_COLS_CHOSEN, chosenColumns);
  }

  public static void setCacheDataDirPrefix(Configuration conf, String dirPrefix)
  {
    conf.set(KEY_DATA_CACHE_DIR_PREFIX, dirPrefix);
  }

  public static void setCacheDataDirSuffix(Configuration conf, String dirSuffix)
  {
    conf.set(KEY_DATA_CACHE_DIR_SUFFIX, dirSuffix);
  }

  public static void setCacheDataEnabled(Configuration conf, boolean cacheEnabled)
  {
    conf.setBoolean(KEY_CACHE_ENABLED, cacheEnabled);
  }

  public static void setCacheDataLocationWhitelist(Configuration conf, String whitelist)
  {
    conf.set(KEY_DATA_CACHE_LOCATION_WHITELIST, whitelist);
  }

  public static void setCacheDataLocationBlacklist(Configuration conf, String blacklist)
  {
    conf.set(KEY_DATA_CACHE_LOCATION_BLACKLIST, blacklist);
  }

  public static void setCacheDataMinColumns(Configuration conf, int minColumns)
  {
    conf.setInt(KEY_DATA_CACHE_TABLE_MIN_COLS, minColumns);
  }

  public static void setCacheDataTable(Configuration conf, String table)
  {
    conf.set(KEY_DATA_CACHE_TABLE, table);
  }

  public static void setCacheDataTableWhitelist(Configuration conf, String tableWhitelist)
  {
    conf.set(KEY_DATA_CACHE_TABLE_WHITELIST, tableWhitelist);
  }

  public static void setIsStrictMode(Configuration conf, boolean isStrictMode)
  {
    conf.setBoolean(KEY_DATA_CACHE_STRICT_MODE, isStrictMode);
  }

  public static void setIsParallelWarmupEnabled(Configuration conf, boolean isParallelWarmupEnabled)
  {
    conf.setBoolean(KEY_DATA_CACHE_STRICT_MODE, isParallelWarmupEnabled);
  }

  public static void setLocalServerPort(Configuration conf, int localServerPort)
  {
    conf.setInt(KEY_LOCAL_SERVER_PORT, localServerPort);
  }

  public static void setMaxDisks(Configuration conf, int maxDisks)
  {
    conf.setInt(KEY_DATA_CACHE_MAX_DISKS, maxDisks);
  }

  public static void setRemoteFetchProcessInterval(Configuration conf, int interval)
  {
    conf.setInt(KEY_REMOTE_FETCH_PROCESS_INTERVAL, interval);
  }

  public static void setServerPort(Configuration conf, int serverPort)
  {
    conf.setInt(KEY_SERVER_PORT, serverPort);
  }
}
