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
package com.qubole.rubix.bookkeeper;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.common.util.concurrent.Service;
import com.qubole.rubix.bookkeeper.utils.DiskUtils;
import com.qubole.rubix.bookkeeper.validation.CachingValidator;
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.core.CachingFileSystemStatsProvider;
import com.qubole.rubix.core.ClusterManagerInitilizationException;
import com.qubole.rubix.core.ReadRequest;
import com.qubole.rubix.core.ReadRequestChainStats;
import com.qubole.rubix.core.RemoteReadRequestChain;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import com.qubole.rubix.spi.thrift.BlockLocation;
import com.qubole.rubix.spi.thrift.BookKeeperService;
import com.qubole.rubix.spi.thrift.CacheStatusRequest;
import com.qubole.rubix.spi.thrift.CacheStatusResponse;
import com.qubole.rubix.spi.thrift.FileInfo;
import com.qubole.rubix.spi.thrift.Location;
import com.qubole.rubix.spi.thrift.ReadResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.DirectBufferPool;
import org.apache.thrift.TException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static com.qubole.rubix.common.metrics.BookKeeperMetrics.CacheMetric.CACHE_AVAILABLE_SIZE_GAUGE;
import static com.qubole.rubix.common.metrics.BookKeeperMetrics.CacheMetric.CACHE_EVICTION_COUNT;
import static com.qubole.rubix.common.metrics.BookKeeperMetrics.CacheMetric.CACHE_EXPIRY_COUNT;
import static com.qubole.rubix.common.metrics.BookKeeperMetrics.CacheMetric.CACHE_HIT_RATE_GAUGE;
import static com.qubole.rubix.common.metrics.BookKeeperMetrics.CacheMetric.CACHE_INVALIDATION_COUNT;
import static com.qubole.rubix.common.metrics.BookKeeperMetrics.CacheMetric.CACHE_MISS_RATE_GAUGE;
import static com.qubole.rubix.common.metrics.BookKeeperMetrics.CacheMetric.CACHE_REQUEST_COUNT;
import static com.qubole.rubix.common.metrics.BookKeeperMetrics.CacheMetric.CACHE_SIZE_GAUGE;
import static com.qubole.rubix.common.metrics.BookKeeperMetrics.CacheMetric.NONLOCAL_REQUEST_COUNT;
import static com.qubole.rubix.common.metrics.BookKeeperMetrics.CacheMetric.REMOTE_REQUEST_COUNT;
import static com.qubole.rubix.common.metrics.BookKeeperMetrics.CacheMetric.TOTAL_REQUEST_COUNT;
import static com.qubole.rubix.core.ReadRequestChainStats.DOWNLOADED_FOR_NON_LOCAL_METRIC;
import static com.qubole.rubix.core.ReadRequestChainStats.DOWNLOADED_FOR_PARALLEL_WARMUP_METRIC;
import static com.qubole.rubix.core.ReadRequestChainStats.EXTRA_READ_FOR_NON_LOCAL_METRIC;
import static com.qubole.rubix.core.ReadRequestChainStats.PARALLEL_DOWNLOAD_TIME_METRIC;
import static com.qubole.rubix.core.ReadRequestChainStats.WARMUP_TIME_NON_LOCAL_METRIC;
import static com.qubole.rubix.spi.CacheUtil.UNKONWN_GENERATION_NUMBER;
import static com.qubole.rubix.spi.utils.DataSizeUnits.BYTES;
import static com.qubole.rubix.spi.utils.DataSizeUnits.MEGABYTES;

/**
 * Created by stagra on 12/2/16.
 */
public abstract class BookKeeper implements BookKeeperService.Iface
{
  private static final int MAX_FILES_EXPECTED = 1000000;
  private static final double FILE_ACCESSED_FILTER_FPP = 0.01;
  private static Log log = LogFactory.getLog(BookKeeper.class);
  private static DirectBufferPool bufferPool = new DirectBufferPool();
  private static final CachingFileSystemStatsProvider warmupStats = new CachingFileSystemStatsProvider();

  protected static Cache<String, FileMetadata> fileMetadataCache;
  private static LoadingCache<String, FileInfo> fileInfoCache;
  protected ClusterManager clusterManager;
  protected final Configuration conf;
  private static Integer lock = 1;
  private long splitSize;
  private RemoteFetchProcessor fetchProcessor;
  private final Ticker ticker;
  private static long totalAvailableForCacheInMB;

  // Registry for gathering & storing necessary metrics
  protected final MetricRegistry metrics;
  private final BookKeeperMetrics bookKeeperMetrics;

  // Metrics to keep track of cache interactions
  private static Counter cacheEvictionCount;
  private static Counter cacheInvalidationCount;
  private static Counter cacheExpiryCount;
  private Counter totalRequestCount;
  private Counter remoteRequestCount;
  private Counter cacheRequestCount;
  private Counter nonlocalRequestCount;

  //  Maintains generation number for remote file
  private Cache<String, Integer> generationNumberCache;
  private BloomFilter fileAccessedBloomFilter;

  public BookKeeper(Configuration conf, BookKeeperMetrics bookKeeperMetrics) throws FileNotFoundException
  {
    this(conf, bookKeeperMetrics, Ticker.systemTicker());
  }

  @Override
  public boolean isBookKeeperAlive()
  {
    return true;
  }

  @VisibleForTesting
  BookKeeper(Configuration conf, BookKeeperMetrics bookKeeperMetrics, Ticker ticker) throws FileNotFoundException
  {
    this.conf = conf;
    this.bookKeeperMetrics = bookKeeperMetrics;
    this.metrics = bookKeeperMetrics.getMetricsRegistry();
    this.ticker = ticker;
    this.splitSize = CacheConfig.getCacheFileSplitSize(conf);
    initializeMetrics();
    initializeCache(conf, ticker);
    cleanupOldCacheFiles(conf);

    fetchProcessor = null;
    if (CacheConfig.isParallelWarmupEnabled(conf)) {
      fetchProcessor = new RemoteFetchProcessor(this, metrics, conf, warmupStats);
    }
  }

  RemoteFetchProcessor getRemoteFetchProcessorInstance()
  {
    return fetchProcessor;
  }

  // Cleanup the cached files that were downloaded as a part of previous bookkeeper session.
  // This makes sure we always start with a clean empty cash.
  // TODO: We need to come up with a way to persist the files being downloaded before
  // So that we can use that info to load those files in guava cache.
  private void cleanupOldCacheFiles(Configuration conf)
  {
    if (CacheConfig.isCleanupFilesDuringStartEnabled(conf)) {
      try {
        int numDisks = CacheConfig.getCacheMaxDisks(conf);
        String dirSuffix = CacheConfig.getCacheDataDirSuffix(conf);
        List<String> dirPrefixList = CacheUtil.getDirPrefixList(conf);

        for (String dirPrefix : dirPrefixList) {
          for (int i = 0; i < numDisks; i++) {
            java.nio.file.Path path = Paths.get(dirPrefix + i, dirSuffix, "*");
            DiskUtils.clearDirectory(path.toString());
          }

          java.nio.file.Path path = Paths.get(dirPrefix, dirSuffix, "*");
          DiskUtils.clearDirectory(path.toString());
        }
      }
      catch (IOException ex) {
        log.error("Could not clean up the old cached files", ex);
      }
    }
  }

  /**
   * Initialize the instruments used for gathering desired metrics.
   */
  private void initializeMetrics()
  {
    cacheEvictionCount = metrics.counter(CACHE_EVICTION_COUNT.getMetricName());
    cacheInvalidationCount = metrics.counter(CACHE_INVALIDATION_COUNT.getMetricName());
    cacheExpiryCount = metrics.counter(CACHE_EXPIRY_COUNT.getMetricName());
    totalRequestCount = metrics.counter(TOTAL_REQUEST_COUNT.getMetricName());
    cacheRequestCount = metrics.counter(CACHE_REQUEST_COUNT.getMetricName());
    nonlocalRequestCount = metrics.counter(NONLOCAL_REQUEST_COUNT.getMetricName());
    remoteRequestCount = metrics.counter(REMOTE_REQUEST_COUNT.getMetricName());

    metrics.register(CACHE_HIT_RATE_GAUGE.getMetricName(), new Gauge<Double>()
    {
      @Override
      public Double getValue()
      {
        return ((double) cacheRequestCount.getCount() + nonlocalRequestCount.getCount()) /
                (cacheRequestCount.getCount() + remoteRequestCount.getCount() + nonlocalRequestCount.getCount());
      }
    });
    metrics.register(CACHE_MISS_RATE_GAUGE.getMetricName(), new Gauge<Double>()
    {
      @Override
      public Double getValue()
      {
        return ((double) remoteRequestCount.getCount() / (cacheRequestCount.getCount() + remoteRequestCount.getCount() + nonlocalRequestCount.getCount()));
      }
    });
    metrics.register(CACHE_SIZE_GAUGE.getMetricName(), new Gauge<Integer>()
    {
      @Override
      public Integer getValue()
      {
        return DiskUtils.getCacheSizeMB(conf);
      }
    });
    metrics.register(CACHE_AVAILABLE_SIZE_GAUGE.getMetricName(), new Gauge<Long>()
    {
      @Override
      public Long getValue()
      {
        return totalAvailableForCacheInMB;
      }
    });
  }

  @Override
  public CacheStatusResponse getCacheStatus(CacheStatusRequest request) throws TException
  {
    try {
      initializeClusterManager(request);
    }
    catch (ClusterManagerInitilizationException ex) {
      log.error("Not able to initialize ClusterManager for cluster type : " +
          ClusterType.findByValue(request.getClusterType()) + " with Exception : " + ex);
      return null;
    }

    ClusterManager.ClusterInfo clusterInfo = clusterManager.getClusterInfo();
    List<String> nodes = clusterInfo.getNodes();
    int currentNodeIndex = clusterInfo.getCurrentNodeIndex();
    if (currentNodeIndex == -1 || nodes == null) {
      log.error("Initialization not done for Cluster Type" + ClusterType.findByValue(request.getClusterType()));
      return null;
    }

    Map<Long, Integer> blockSplits = new HashMap<>();
    long blockNumber = 0;

    long fileLength = request.getFileLength();
    String remotePath = request.getRemotePath();
    long lastModified = request.getLastModified();
    long startBlock = request.getStartBlock();
    long endBlock = request.getEndBlock();
    boolean incrMetrics = request.isIncrMetrics();

    for (long i = 0; i < fileLength; i = i + splitSize) {
      long end = i + splitSize;
      if (end > fileLength) {
        end = fileLength;
      }
      String key = remotePath + i + end;
      int nodeIndex = clusterManager.getNodeIndex(nodes.size(), key);
      blockSplits.put(blockNumber, nodeIndex);
      blockNumber++;
    }

    FileMetadata md;
    //  If multiple threads call get of guava cache, then all but one get blocked and computation is done for it
    //  and values is returned to other blocked threads.
    try {
      md = fileMetadataCache.get(remotePath, () -> new FileMetadata(
              remotePath,
              fileLength,
              lastModified,
              0,
              conf,
              generationNumberCache,
              fileAccessedBloomFilter));
      if (isInvalidationRequired(md.getLastModified(), lastModified)) {
        invalidateFileMetadata(remotePath);
        md = fileMetadataCache.get(remotePath, () -> new FileMetadata(
                remotePath,
                fileLength,
                lastModified,
                0,
                conf,
                generationNumberCache,
                fileAccessedBloomFilter));
      }
    }
    catch (ExecutionException e) {
      log.error(String.format("Could not fetch Metadata for %s", remotePath), e);
      throw new TException(e);
    }
    endBlock = setCorrectEndBlock(endBlock, fileLength, remotePath);
    List<BlockLocation> blockLocations = new ArrayList<>((int) (endBlock - startBlock));
    int blockSize = CacheConfig.getBlockSize(conf);

    //TODO: Store Node indices too, i.e. split to nodeIndex map and compare indices here instead of strings(nodenames).
    int totalRequests = 0;
    int cacheRequests = 0;
    int remoteRequests = 0;
    int nonLocalRequests = 0;

    try {
      for (long blockNum = startBlock; blockNum < endBlock; blockNum++) {
        totalRequests++;

        long split = (blockNum * blockSize) / splitSize;
        if (!blockSplits.get(split).equals(currentNodeIndex)) {
          blockLocations.add(new BlockLocation(Location.NON_LOCAL, nodes.get(blockSplits.get(split))));
          nonLocalRequests++;
        }
        else {
          if (md.isBlockCached(blockNum)) {
            blockLocations.add(new BlockLocation(Location.CACHED, nodes.get(blockSplits.get(split))));
            cacheRequests++;
          }
          else {
            blockLocations.add(new BlockLocation(Location.LOCAL, nodes.get(blockSplits.get(split))));
            remoteRequests++;
          }
        }
      }
    }
    catch (IOException e) {
      throw new TException(e);
    }

    if (request.isIncrMetrics() && !isValidatingCachingBehavior(remotePath)) {
      totalRequestCount.inc(totalRequests);
      nonlocalRequestCount.inc(nonLocalRequests);
      cacheRequestCount.inc(cacheRequests);
      remoteRequestCount.inc(remoteRequests);
    }
    return new CacheStatusResponse(blockLocations, md.getGenerationNumber());
  }

  public boolean isInitialized()
  {
    return clusterManager != null;
  }

  private void initializeClusterManager(CacheStatusRequest request) throws ClusterManagerInitilizationException
  {
    if (!isInitialized()) {
      checkState(request.isSetClusterType(), "Received getCacheStatus without clusterType before BookKeeper is initialized");
    }

    initializeClusterManager(request.getClusterType());
  }

  @VisibleForTesting
  public void initializeClusterManager(int clusterType) throws ClusterManagerInitilizationException
  {
    if (!isInitialized()) {
      ClusterManager manager = null;
      synchronized (lock) {
        if (!isInitialized()) {
          manager = getClusterManagerInstance(ClusterType.findByValue(clusterType), conf);
          try {
            manager.initialize(conf);
          }
          catch (UnknownHostException e) {
            throw new ClusterManagerInitilizationException("Unable to initialize cluster manager", e);
          }
          this.clusterManager = manager;
        }
      }
    }
  }

  @VisibleForTesting
  public ClusterManager getClusterManagerInstance(ClusterType clusterType, Configuration config)
      throws ClusterManagerInitilizationException
  {
    String clusterManagerClassName = CacheConfig.getClusterManagerClass(conf, clusterType);
    log.debug("Initializing cluster manager : " + clusterManagerClassName);
    ClusterManager manager = null;

    try {
      Class clusterManagerClass = conf.getClassByName(clusterManagerClassName);
      Constructor constructor = clusterManagerClass.getConstructor();
      manager = (ClusterManager) constructor.newInstance();
    }
    catch (ClassNotFoundException | NoSuchMethodException | InstantiationException |
        IllegalAccessException | InvocationTargetException ex) {
      String errorMessage = String.format("Not able to initialize ClusterManager class : %s ",
          clusterManagerClassName);
      log.error(errorMessage);
      throw new ClusterManagerInitilizationException(errorMessage, ex);
    }

    return manager;
  }

  @Override
  public void setAllCached(String remotePath, long fileLength, long lastModified, long startBlock, long endBlock, int generationNumber)
      throws TException
  {
    FileMetadata md;
    md = fileMetadataCache.getIfPresent(remotePath);

    //md will be null when 2 users try to update the file in parallel and both their entries are invalidated.
    // different generation number means invalidation has occurred
    // TODO: find a way to optimize this so that the file doesn't have to be read again in next request (new data is stored instead of invalidation)
    if (md == null) {
      log.debug(String.format("Could not update the metadata for file %s", remotePath));
      return;
    }
    if (md.getGenerationNumber() != generationNumber)
    {
      log.debug(String.format("Could not update the metadata for file %s having different generationNumber %d in cache and %d in request", remotePath, md.getGenerationNumber(), generationNumber));
      return;
    }
    if (isInvalidationRequired(md.getLastModified(), lastModified)) {
      invalidateFileMetadata(remotePath);
      return;
    }
    endBlock = setCorrectEndBlock(endBlock, fileLength, remotePath);
    log.debug("Updating cache for " + remotePath + " StartBlock : " + startBlock + " EndBlock : " + endBlock);

    try {
      OptionalInt updatedBlocks = md.setBlocksCached(startBlock, endBlock);
      if (updatedBlocks.isPresent()) {
        long currentFileSize = md.incrementCurrentFileSize(updatedBlocks.getAsInt() * CacheConfig.getBlockSize(conf));
        // CurrentFileSize as per Blocks' based computation can cross actual file size
        // This can only happen when the last block of file is not completely full
        // as it doesnt align to the block boundary
        currentFileSize = Math.min(currentFileSize, fileLength);
        replaceFileMetadata(remotePath, currentFileSize, conf);
      }
    }
    catch (IOException e) {
      throw new TException(e);
    }
  }

  @VisibleForTesting
  public long getTotalCacheWeight()
  {
    return fileMetadataCache.asMap().values().stream().mapToLong(FileMetadata::getWeight).sum();
  }

  @Override
  public Map<String, Double> getCacheMetrics()
  {
    ImmutableMap.Builder<String, Double> cacheMetrics = ImmutableMap.builder();

    // Clean up cache to resolve any pending changes.
    fileMetadataCache.cleanUp();

    // Add all enabled metrics gauges
    for (Map.Entry<String, Gauge> gaugeEntry : metrics.getGauges(bookKeeperMetrics.getMetricsFilter()).entrySet()) {
      try {
        cacheMetrics.put(gaugeEntry.getKey(), getGaugeValueAsDouble(gaugeEntry.getValue().getValue()));
      }
      catch (ClassCastException e) {
        log.error(String.format("Gauge metric %s is not a numeric value", gaugeEntry.getKey()), e);
      }
    }

    // Add all enabled metrics counters
    for (Map.Entry<String, Counter> counterEntry : metrics.getCounters(bookKeeperMetrics.getMetricsFilter()).entrySet()) {
      cacheMetrics.put(counterEntry.getKey(), (double) counterEntry.getValue().getCount());
    }

    return cacheMetrics.build();
  }

  @Override
  public Map<String, Long> getReadRequestChainStats()
  {
    ImmutableMap.Builder<String, Long> stats = ImmutableMap.builder();
    ReadRequestChainStats readRequestChainStats = warmupStats.getStats();
    // Differentiate parallel warmup case because in that case caching for data owned by current node is also done by RemoteFetchProcessor
    // While in the case of read-through warmup, caching for data owned by current node is done by the client
    // Hence DOWNLOADED_FOR_PARALLEL_WARMUP contains data for both local and non-local warmup
    if (CacheConfig.isParallelWarmupEnabled(conf)) {
      stats.put(DOWNLOADED_FOR_PARALLEL_WARMUP_METRIC, readRequestChainStats.getRemoteRRCDataRead());
      stats.put(PARALLEL_DOWNLOAD_TIME_METRIC, readRequestChainStats.getRemoteRRCWarmupTime());
    }
    else {
      stats.put(DOWNLOADED_FOR_NON_LOCAL_METRIC, readRequestChainStats.getRemoteRRCDataRead());
      stats.put(EXTRA_READ_FOR_NON_LOCAL_METRIC, readRequestChainStats.getRemoteRRCExtraDataRead());
      stats.put(WARMUP_TIME_NON_LOCAL_METRIC, readRequestChainStats.getRemoteRRCWarmupTime());
    }
    return stats.build();
  }

  //This method is to ensure that data required by another node is cached before it is read by that node
  //using localTransferServer.
  // If the data is not already cached, remoteReadRequest for that block is sent and data is cached.

  //TODO : buffer is initialized everytime readData is called and it contains garbage value which is not required.
  // We cannot make it static because the variable is used in remoteReadRequestChain to write (cache) data to files.
  // So, it has to store the correct value in each thread. getCacheStatus is called twice.
  @Override
  public ReadResponse readData(String remotePath, long offset, int length, long fileSize, long lastModified, int clusterType)
      throws TException
  {
    if (CacheConfig.isParallelWarmupEnabled(conf)) {
      startRemoteFetchProcessor();
      log.debug("Adding to the queue Path : " + remotePath + " Offset : " + offset + " Length " + length);
      fetchProcessor.addToProcessQueue(remotePath, offset, length, fileSize, lastModified);
      return new ReadResponse(false, UNKONWN_GENERATION_NUMBER);
    }
    else {
      return readDataInternal(remotePath, offset, length, fileSize, lastModified, clusterType);
    }
  }

  private synchronized void startRemoteFetchProcessor()
  {
    if (fetchProcessor.state() == Service.State.NEW) {
      fetchProcessor.startAsync();
    }
  }

  @Override
  public FileInfo getFileInfo(String remotePath) throws TException
  {
    try {
      return fileInfoCache.get(remotePath);
    }
    catch (ExecutionException e) {
      log.error(String.format("Could not fetch FileInfo from Cache for %s", remotePath), e);
      throw new TException(e);
    }
  }

  private ReadResponse readDataInternal(String remotePath,
      long offset,
      int length,
      long fileSize,
      long lastModified,
      int clusterType) throws TException
  {
    int blockSize = CacheConfig.getBlockSize(conf);
    byte[] buffer = new byte[blockSize];
    FileSystem fs = null;
    FSDataInputStream inputStream = null;
    Path path = new Path(remotePath);
    long startBlock = offset / blockSize;
    long endBlock = ((offset + (length - 1)) / CacheConfig.getBlockSize(conf)) + 1;
    CacheStatusResponse response = null;
    try {
      int idx = 0;
      CacheStatusRequest request = new CacheStatusRequest(remotePath, fileSize, lastModified, startBlock, endBlock).setClusterType(clusterType);
      response = getCacheStatus(request);
      List<BlockLocation> blockLocations = response.getBlocks();

      for (long blockNum = startBlock; blockNum < endBlock; blockNum++, idx++) {
        long readStart = blockNum * blockSize;
        log.debug(" blockLocation is: " + blockLocations.get(idx).getLocation() + " for path " + remotePath + " offset " + offset + " length " + length);
        if (blockLocations.get(idx).getLocation() != Location.CACHED) {
          if (fs == null) {
            fs = path.getFileSystem(conf);
            fs.initialize(path.toUri(), conf);

            inputStream = fs.open(path, blockSize);
          }

          // Cache the data
          // Ue RRRC directly instead of creating instance of CachingFS as in certain circumstances, CachingFS could
          // send this request to NonLocalRRC which would be wrong as that would not cache it on disk
          long expectedBytesToRead = (readStart + blockSize) > fileSize ? (fileSize - readStart) : blockSize;
          RemoteReadRequestChain remoteReadRequestChain = new RemoteReadRequestChain(inputStream, remotePath, response.getGenerationNumber(), bufferPool, conf, buffer, new BookKeeperFactory(this));
          remoteReadRequestChain.addReadRequest(new ReadRequest(readStart, readStart + expectedBytesToRead, readStart, readStart + expectedBytesToRead, buffer, 0, fileSize));
          remoteReadRequestChain.lock();
          long dataRead = remoteReadRequestChain.call();
          // Making sure the data downloaded matches with the expected bytes. If not, there is some problem with
          // the download this time. So won't update the cache metadata and return false so that client can
          // fall back on the directread
          if (dataRead == expectedBytesToRead) {
            remoteReadRequestChain.updateCacheStatus(remotePath, fileSize, lastModified, blockSize, conf);
            warmupStats.addReadRequestChainStats(remoteReadRequestChain.getStats());
          }
          else {
            log.error("Not able to download requested bytes. Not updating the cache for block " + blockNum);
            return new ReadResponse(false, response.getGenerationNumber());
          }
        }
      }
      return new ReadResponse(true, response.getGenerationNumber());
    }
    catch (Exception e) {
      log.warn("Could not cache data: ", e);
      return new ReadResponse(false, response.getGenerationNumber());
    }
    finally {
      if (inputStream != null) {
        try {
          inputStream.close();
        }
        catch (IOException e) {
          log.error("Error closing inputStream", e);
        }
      }
    }
  }

  private long setCorrectEndBlock(long endBlock, long fileLength, String remotePath)
  {
    long lastBlock = (fileLength - 1) / CacheConfig.getBlockSize(conf);
    if (endBlock > (lastBlock + 1)) {
      endBlock = lastBlock + 1;
    }

    return endBlock;
  }

  private synchronized void initializeCache(final Configuration conf, final Ticker ticker)
      throws FileNotFoundException
  {
    if (CacheConfig.isOnMaster(conf) && !CacheConfig.isCacheDataOnMasterEnabled(conf)) {
      log.info("Cache disabled on master node; skipping initialization");
      totalAvailableForCacheInMB = 0;
      fileInfoCache = CacheBuilder.newBuilder().build(
              new CacheLoader<String, FileInfo>()
              {
                @Override
                public FileInfo load(String key) throws Exception
                {
                  throw new UnsupportedOperationException(
                          String.format("unexpected load call for key %s; cache disabled on master node", key));
                }
              });
      fileMetadataCache = new ThrowingEmptyCache<>();
      return;
    }

    CacheUtil.createCacheDirectories(conf);

    int cacheDiskCount = CacheUtil.getCacheDiskCount(conf);
    if (cacheDiskCount == 0) {
      throw new IllegalStateException("No disks available for caching");
    }

    long avail = 0;
    for (int d = 0; d < cacheDiskCount; d++) {
      avail += new File(CacheUtil.getDirPath(d, conf)).getUsableSpace();
    }
    avail = BYTES.toMB(avail);

    // In corner cases evictions might not make enough space for new entries
    // To minimize those cases, consider available space lower than actual
    final long totalUsableMBs = (long) (0.95 * avail);

    final long cacheMaxSizeInMB = CacheConfig.getCacheDataFullnessMaxSizeInMB(conf);
    totalAvailableForCacheInMB = (cacheMaxSizeInMB == 0)
        ? (long) (totalUsableMBs * 1.0 * CacheConfig.getCacheDataFullnessPercentage(conf) / 100.0)
        : cacheMaxSizeInMB;
    log.info("total free space " + avail + "MB, maximum size of cache " + totalAvailableForCacheInMB + "MB");

    initializeFileInfoCache(conf, ticker);

    fileMetadataCache = CacheBuilder.newBuilder()
        .ticker(ticker)
        .weigher((Weigher<String, FileMetadata>) (key, md) -> md.getWeight())
        .maximumWeight(MEGABYTES.toKB(totalAvailableForCacheInMB))
        .expireAfterWrite(CacheConfig.getCacheDataExpirationAfterWrite(conf), TimeUnit.MILLISECONDS)
        .removalListener(new CacheRemovalListener())
        .build();
    fileAccessedBloomFilter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), MAX_FILES_EXPECTED, FILE_ACCESSED_FILTER_FPP);
    generationNumberCache = CacheBuilder.newBuilder()
            .expireAfterAccess(2, TimeUnit.HOURS)
            .build();

  }

  @VisibleForTesting
  public FileMetadata getFileMetadata(String key)
  {
    return fileMetadataCache.getIfPresent(key);
  }

  private static void initializeFileInfoCache(final Configuration conf, final Ticker ticker)
  {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    int expiryPeriod = CacheConfig.getStaleFileInfoExpiryPeriod(conf);
    fileInfoCache = CacheBuilder.newBuilder()
        .ticker(ticker)
        .expireAfterWrite(expiryPeriod, TimeUnit.SECONDS)
        .removalListener(new RemovalListener<String, FileInfo>()
        {
          @Override
          public void onRemoval(RemovalNotification<String, FileInfo> notification)
          {
            log.debug("Removed FileInfo for path " + notification.getKey() + " due to " + notification.getCause());
          }
        })
        .build(CacheLoader.asyncReloading(new CacheLoader<String, FileInfo>()
        {
          @Override
          public FileInfo load(String s) throws Exception
          {
            Path path = new Path(s);
            FileSystem fs = path.getFileSystem(conf);
            FileStatus status = fs.getFileStatus(path);
            FileInfo info = new FileInfo(status.getLen(), status.getModificationTime());
            return info;
          }
        }, executor));
  }

  protected static class CacheRemovalListener implements RemovalListener<String, FileMetadata>
  {
    @Override
    public void onRemoval(RemovalNotification<String, FileMetadata> notification)
    {
      FileMetadata md = notification.getValue();
      md.closeAndCleanup(notification.getCause(), fileMetadataCache);
      if (!isValidatingCachingBehavior(md.getRemotePath())) {
        switch (notification.getCause()) {
          case EXPLICIT:
            cacheInvalidationCount.inc();
            break;
          case SIZE:
            cacheEvictionCount.inc();
            break;
          case EXPIRED:
            cacheExpiryCount.inc();
            break;
          default:
            break;
        }
      }
    }
  }

  // This method is to invalidate FileMetadata from guava cache.
  @Override
  public void invalidateFileMetadata(String key)
  {
    // We might come in here with cache not initialized e.g. fs.create
    if (fileMetadataCache != null) {
      fileMetadataCache.invalidate(key);
    }
  }

  private void replaceFileMetadata(String key, long currentFileSize, Configuration conf)
  {
    if (fileMetadataCache != null) {
      FileMetadata metadata = fileMetadataCache.getIfPresent(key);
      if (metadata != null) {
        FileMetadata newMetaData = new FileMetadata(key,
            metadata.getFileSize(),
            metadata.getLastModified(),
            currentFileSize,
            conf,
            metadata.getGenerationNumber());
        fileMetadataCache.put(key, newMetaData);
      }
    }
  }

  private boolean isInvalidationRequired(long metadataLastModifiedTime, long remoteLastModifiedTime)
  {
    return CacheConfig.isFileStalenessCheckEnabled(conf) && (metadataLastModifiedTime != remoteLastModifiedTime);
  }

  private static boolean isValidatingCachingBehavior(String remotePath)
  {
    return CachingValidator.VALIDATOR_TEST_FILE_NAME.equals(CacheUtil.getName(remotePath));
  }

  /**
   * Convert the provided gauge value to a {@code double}.
   *
   * @param gaugeValue The gauge value to convert.
   * @return The gauge value as a {@code double}.
   */
  private double getGaugeValueAsDouble(Object gaugeValue)
  {
    if (gaugeValue instanceof Long) {
      return ((Long) gaugeValue).doubleValue();
    }
    else if (gaugeValue instanceof Integer) {
      return ((Integer) gaugeValue).doubleValue();
    }
    else if (Double.isNaN((double) gaugeValue)) {
      return Double.NaN;
    }
    else {
      throw new ClassCastException("Could not cast gauge metric value type to Double");
    }
  }
}
