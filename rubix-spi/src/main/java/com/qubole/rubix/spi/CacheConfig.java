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
  private static final String KEY_HEARTBEAT_INITIAL_DELAY = "rubix.monitor.heartbeat.initial.delay";
  private static final String KEY_HEARTBEAT_INTERVAL = "rubix.monitor.heartbeat.interval";
  private static final String KEY_LOCAL_TRANSFER_BUFFER_SIZE = "hadoop.cache.data.buffer.size";
  private static final String KEY_LOCAL_SERVER_PORT = "hadoop.cache.data.local.server.port";
  private static final String KEY_MAX_RETRIES = "hadoop.cache.data.client.num-retries";
  private static final String KEY_METRICS_CACHE_ENABLED = "rubix.metrics.cache.enabled";
  private static final String KEY_METRICS_LIVENESS_ENABLED = "rubix.metrics.liveness.enabled";
  private static final String KEY_METRICS_JVM_ENABLED = "rubix.metrics.jvm.enabled";
  private static final String KEY_METRICS_STATSD_HOST = "rubix.metrics.statsd.host";
  private static final String KEY_METRICS_STATSD_INTERVAL = "rubix.metrics.statsd.interval";
  private static final String KEY_METRICS_STATSD_PORT = "rubix.metrics.statsd.port";
  private static final String KEY_METRICS_REPORTERS = "rubix.metrics.reporters";
  private static final String KEY_PARALLEL_WARMUP = "rubix.parallel.warmup";
  private static final String KEY_PROCESS_THREAD_INITIAL_DELAY = "rubix.request.process.initial.delay";
  private static final String KEY_PROCESS_THREAD_INTERVAL = "rubix.request.process.interval";
  private static final String KEY_REMOTE_FETCH_PROCESS_INTERVAL = "rubix.remotefetch.interval";
  private static final String KEY_REMOTE_FETCH_THREADS = "rubix.remotefetch.threads";
  private static final String KEY_RUBIX_ON_MASTER = "rubix.cluster.on-master";
  private static final String KEY_SERVER_PORT = "hadoop.cache.data.bookkeeper.port";
  private static final String KEY_SERVER_MAX_THREADS = "hadoop.cache.data.bookkeeper.max-threads";
  private static final String KEY_SERVICE_RETRY_INTERVAL = "rubix.network.service.retry-interval";
  private static final String KEY_SERVICE_MAX_RETRIES = "rubix.network.service.max-retries";
  private static final String KEY_SOCKET_READ_TIMEOUT = "hadoop.cache.network.socket.read.timeout";
  private static final String KEY_WORKER_LIVENESS_EXPIRY = "rubix.monitor.worker.liveness.expiry";
  private static final String KEY_PRESTO_CLUSTER_MANAGER = "rubix.presto.clustermanager.class";
  private static final String KEY_HADOOP_CLUSTER_MANAGER = "rubix.hadoop.clustermanager.class";
  private static final String KEY_DUMMY_CLUSTER_MANAGER = "rubix.dummy.clustermanager.class";
  private static final String KEY_ENABLE_FILE_STALESSNESS_CHECK = "rubix.enable.file.staleness-check";
  private static final String KEY_STALE_FILEINFO_EXPIRY_PERIOD = "rubix.stale.fileinfo.expiry.period";
  private static final String KEY_CLEANUP_FILES_DURING_START = "rubix.cleanup.files.during.start";

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
  private static final int DEFAULT_HEARTBEAT_INITIAL_DELAY = 30000; // ms
  private static final int DEFAULT_HEARTBEAT_INTERVAL = 30000; // ms
  private static final int DEFAULT_LOCAL_TRANSFER_BUFFER_SIZE = 10 * 1024 * 1024; // 10MB
  private static final int DEFAULT_LOCAL_SERVER_PORT = 8898;
  private static final int DEFAULT_MAX_BUFFER_SIZE = 1024;
  private static final int DEFAULT_MAX_RETRIES = 3;
  private static final boolean DEFAULT_METRICS_CACHE_ENABLED = true;
  private static final boolean DEFAULT_METRICS_LIVENESS_ENABLED = true;
  private static final boolean DEFAULT_METRICS_JVM_ENABLED = false;
  private static final String DEFAULT_METRICS_STATSD_HOST = "127.0.0.1"; // localhost
  private static final int DEFAULT_METRICS_STATSD_INTERVAL = 10000; // ms
  private static final int DEFAULT_METRICS_STATSD_PORT = 8125; // default StatsD port
  private static final String DEFAULT_METRICS_REPORTERS = "JMX";
  private static final boolean DEFAULT_PARALLEL_WARMUP = true;
  private static final int DEFAULT_PROCESS_THREAD_INITIAL_DELAY = 1000; // ms
  private static final int DEFAULT_PROCESS_THREAD_INTERVAL = 1000; // ms
  private static final int DEFAULT_REMOTE_FETCH_PROCESS_INTERVAL = 10000; // ms
  private static final int DEFAULT_REMOTE_FETCH_THREADS = 10;
  private static final boolean DEFAULT_RUBIX_ON_MASTER = false;
  private static final int DEFAULT_SERVER_MAX_THREADS = Integer.MAX_VALUE;
  private static final int DEFAULT_SERVER_PORT = 8899;
  private static final int DEFAULT_SERVICE_RETRY_INTERVAL = 30000; // ms
  private static final int DEFAULT_SERVICE_MAX_RETRIES = 100;
  private static final int DEFAULT_SOCKET_READ_TIMEOUT = 30000; // ms
  private static final int DEFAULT_WORKER_LIVENESS_EXPIRY = 60000; // ms
  private static final int DEFAULT_WORKER_LIVENESS_METRIC_INITIAL_DELAY = 30000; // ms
  private static final int DEFAULT_WORKER_LIVENESS_METRIC_INTERVAL = 30000; // ms
  private static final String DEFAULT_PRESTO_CLUSTER_MANAGER = "com.qubole.rubix.presto.PrestoClusterManager";
  private static final String DEFAULT_HADOOP_CLUSTER_MANAGER = "com.qubole.rubix.hadoop2.Hadoop2ClusterManager";
  private static final String DEFAULT_DUMMY_CLUSTER_MANAGER = "com.qubole.rubix.core.utils.DummyClusterManager";
  private static final boolean DEFAULT_ENABLE_FILE_STALESSNESS_CHECK = true;
  private static final int DEFAULT_STALE_FILEINFO_EXPIRY_PERIOD = 36000; // seconds
  private static final boolean DEFAULT_CLEANUP_FILES_DURING_START = true;

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

  public static int getHeartbeatInitialDelay(Configuration conf)
  {
    return conf.getInt(KEY_HEARTBEAT_INITIAL_DELAY, DEFAULT_HEARTBEAT_INITIAL_DELAY);
  }

  public static int getHeartbeatInterval(Configuration conf)
  {
    return conf.getInt(KEY_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_INTERVAL);
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

  public static String getMetricsReporters(Configuration conf)
  {
    return conf.get(KEY_METRICS_REPORTERS, DEFAULT_METRICS_REPORTERS);
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

  public static int getServiceMaxRetries(Configuration conf)
  {
    return conf.getInt(KEY_SERVICE_MAX_RETRIES, DEFAULT_SERVICE_MAX_RETRIES);
  }

  public static int getServiceRetryInterval(Configuration conf)
  {
    return conf.getInt(KEY_SERVICE_RETRY_INTERVAL, DEFAULT_SERVICE_RETRY_INTERVAL);
  }

  public static int getSocketReadTimeOut(Configuration conf)
  {
    return conf.getInt(KEY_SOCKET_READ_TIMEOUT, DEFAULT_SOCKET_READ_TIMEOUT);
  }

  public static String getStatsDMetricsHost(Configuration conf)
  {
    return conf.get(KEY_METRICS_STATSD_HOST, DEFAULT_METRICS_STATSD_HOST);
  }

  public static int getStatsDMetricsInterval(Configuration conf)
  {
    return conf.getInt(KEY_METRICS_STATSD_INTERVAL, DEFAULT_METRICS_STATSD_INTERVAL);
  }

  public static int getStatsDMetricsPort(Configuration conf)
  {
    return conf.getInt(KEY_METRICS_STATSD_PORT, DEFAULT_METRICS_STATSD_PORT);
  }

  public static int getWorkerLivenessExpiry(Configuration conf)
  {
    return conf.getInt(KEY_WORKER_LIVENESS_EXPIRY, DEFAULT_WORKER_LIVENESS_EXPIRY);
  }

  public static boolean isCacheDataEnabled(Configuration conf)
  {
    return conf.getBoolean(KEY_CACHE_ENABLED, DEFAULT_DATA_CACHE_ENABLED);
  }

  public static boolean areCacheMetricsEnabled(Configuration conf)
  {
    return conf.getBoolean(KEY_METRICS_CACHE_ENABLED, DEFAULT_METRICS_CACHE_ENABLED);
  }

  public static boolean areLivenessMetricsEnabled(Configuration conf)
  {
    return conf.getBoolean(KEY_METRICS_LIVENESS_ENABLED, DEFAULT_METRICS_LIVENESS_ENABLED);
  }

  public static boolean areJvmMetricsEnabled(Configuration conf)
  {
    return conf.getBoolean(KEY_METRICS_JVM_ENABLED, DEFAULT_METRICS_JVM_ENABLED);
  }

  public static boolean isOnMaster(Configuration conf)
  {
    return conf.getBoolean(KEY_RUBIX_ON_MASTER, DEFAULT_RUBIX_ON_MASTER);
  }

  public static boolean isStrictMode(Configuration conf)
  {
    return conf.getBoolean(KEY_DATA_CACHE_STRICT_MODE, DEFAULT_DATA_CACHE_STRICT_MODE);
  }

  public static boolean isParallelWarmupEnabled(Configuration conf)
  {
    return conf.getBoolean(KEY_PARALLEL_WARMUP, DEFAULT_PARALLEL_WARMUP);
  }

  public static String getPrestoClusterManager(Configuration conf)
  {
    return conf.get(KEY_PRESTO_CLUSTER_MANAGER, DEFAULT_PRESTO_CLUSTER_MANAGER);
  }

  public static String getHadoopClusterManager(Configuration conf)
  {
    return conf.get(KEY_HADOOP_CLUSTER_MANAGER, DEFAULT_HADOOP_CLUSTER_MANAGER);
  }

  public static String getDummyClusterManager(Configuration conf)
  {
    return conf.get(KEY_DUMMY_CLUSTER_MANAGER, DEFAULT_DUMMY_CLUSTER_MANAGER);
  }

  public static String getClusterManagerClass(Configuration conf, ClusterType clusterType)
  {
    switch (clusterType) {
      case HADOOP2_CLUSTER_MANAGER:
        return conf.get(KEY_HADOOP_CLUSTER_MANAGER, DEFAULT_HADOOP_CLUSTER_MANAGER);
      case PRESTO_CLUSTER_MANAGER:
        return conf.get(KEY_PRESTO_CLUSTER_MANAGER, DEFAULT_PRESTO_CLUSTER_MANAGER);
      case TEST_CLUSTER_MANAGER:
      case TEST_CLUSTER_MANAGER_MULTINODE:
        return conf.get(KEY_DUMMY_CLUSTER_MANAGER, DEFAULT_DUMMY_CLUSTER_MANAGER);
      default:
        return null;
    }
  }

  public static boolean isFileStalenessCheckEnabled(Configuration conf)
  {
    return conf.getBoolean(KEY_ENABLE_FILE_STALESSNESS_CHECK, DEFAULT_ENABLE_FILE_STALESSNESS_CHECK);
  }

  public static int getStaleFileInfoExpiryPeriod(Configuration conf)
  {
    return conf.getInt(KEY_STALE_FILEINFO_EXPIRY_PERIOD, DEFAULT_STALE_FILEINFO_EXPIRY_PERIOD);
  }

  public static boolean isCleanupFilesDuringStartEnabled(Configuration conf)
  {
    return conf.getBoolean(KEY_CLEANUP_FILES_DURING_START, DEFAULT_CLEANUP_FILES_DURING_START);
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

  public static void setCacheDataExpirationAfterWrite(Configuration conf, int expiryAfterWrite)
  {
    conf.setInt(KEY_DATA_CACHE_EXPIRY_AFTER_WRITE, expiryAfterWrite);
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

  public static void setCacheMetricsEnabled(Configuration conf, boolean cacheMetricsEnabled)
  {
    conf.setBoolean(KEY_METRICS_CACHE_ENABLED, cacheMetricsEnabled);
  }

  public static void setHeartbeatInitialDelay(Configuration conf, int initialDelay)
  {
    conf.setInt(KEY_HEARTBEAT_INITIAL_DELAY, initialDelay);
  }

  public static void setHeartbeatInterval(Configuration conf, int interval)
  {
    conf.setInt(KEY_HEARTBEAT_INTERVAL, interval);
  }

  public static void setIsStrictMode(Configuration conf, boolean isStrictMode)
  {
    conf.setBoolean(KEY_DATA_CACHE_STRICT_MODE, isStrictMode);
  }

  public static void setIsParallelWarmupEnabled(Configuration conf, boolean isParallelWarmupEnabled)
  {
    conf.setBoolean(KEY_PARALLEL_WARMUP, isParallelWarmupEnabled);
  }

  public static void setJvmMetricsEnabled(Configuration conf, boolean jvmMetricsEnabled)
  {
    conf.setBoolean(KEY_METRICS_JVM_ENABLED, jvmMetricsEnabled);
  }

  public static void setLivenessMetricsEnabled(Configuration conf, boolean livenessMetricsEnabled)
  {
    conf.setBoolean(KEY_METRICS_LIVENESS_ENABLED, livenessMetricsEnabled);
  }

  public static void setLocalServerPort(Configuration conf, int localServerPort)
  {
    conf.setInt(KEY_LOCAL_SERVER_PORT, localServerPort);
  }

  public static void setMaxDisks(Configuration conf, int maxDisks)
  {
    conf.setInt(KEY_DATA_CACHE_MAX_DISKS, maxDisks);
  }

  public static void setMetricsReporters(Configuration conf, String reporters)
  {
    conf.set(KEY_METRICS_REPORTERS, reporters);
  }

  public static void setOnMaster(Configuration conf, boolean onMaster)
  {
    conf.setBoolean(KEY_RUBIX_ON_MASTER, onMaster);
  }

  public static void setRemoteFetchProcessInterval(Configuration conf, int interval)
  {
    conf.setInt(KEY_REMOTE_FETCH_PROCESS_INTERVAL, interval);
  }

  public static void setServerPort(Configuration conf, int serverPort)
  {
    conf.setInt(KEY_SERVER_PORT, serverPort);
  }

  public static void setServiceMaxRetries(Configuration conf, int maxRetries)
  {
    conf.setInt(KEY_SERVICE_MAX_RETRIES, maxRetries);
  }

  public static void setServiceRetryInterval(Configuration conf, int retryInterval)
  {
    conf.setInt(KEY_SERVICE_RETRY_INTERVAL, retryInterval);
  }

  public static void setStatsDMetricsHost(Configuration conf, String hostname)
  {
    conf.set(KEY_METRICS_STATSD_HOST, hostname);
  }

  public static void setStatsDMetricsInterval(Configuration conf, int interval)
  {
    conf.setInt(KEY_METRICS_STATSD_INTERVAL, interval);
  }

  public static void setStatsDMetricsPort(Configuration conf, int port)
  {
    conf.setInt(KEY_METRICS_STATSD_PORT, port);
  }

  public static void setWorkerLivenessExpiry(Configuration conf, int expiryTime)
  {
    conf.setInt(KEY_WORKER_LIVENESS_EXPIRY, expiryTime);
  }

  public static void setPrestoClusterManager(Configuration conf, String clusterManager)
  {
    conf.set(KEY_PRESTO_CLUSTER_MANAGER, clusterManager);
  }

  public static void setHadoopClusterManager(Configuration conf, String clusterManager)
  {
    conf.set(KEY_HADOOP_CLUSTER_MANAGER, clusterManager);
  }

  public static void setDummyClusterManager(Configuration conf, String clusterManager)
  {
    conf.set(KEY_DUMMY_CLUSTER_MANAGER, clusterManager);
  }

  public static void setFileStalenessCheck(Configuration conf, boolean fileStalenessCheck)
  {
    conf.setBoolean(KEY_ENABLE_FILE_STALESSNESS_CHECK, fileStalenessCheck);
  }

  public static void setStaleFileInfoExpiryPeriod(Configuration conf, int expiryPeriod)
  {
    conf.setInt(KEY_STALE_FILEINFO_EXPIRY_PERIOD, expiryPeriod);
  }

  public static void setCleanupFilesDuringStart(Configuration conf, boolean isCleanupRequired)
  {
    conf.setBoolean(KEY_CLEANUP_FILES_DURING_START, isCleanupRequired);
  }
}
