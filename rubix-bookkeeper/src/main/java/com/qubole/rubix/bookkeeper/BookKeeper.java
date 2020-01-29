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
import com.google.common.base.Throwables;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.qubole.rubix.bookkeeper.exception.BookKeeperInitializationException;
import com.qubole.rubix.bookkeeper.utils.ConsistentHashUtil;
import com.qubole.rubix.bookkeeper.utils.DiskUtils;
import com.qubole.rubix.bookkeeper.validation.CachingValidator;
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.core.ReadRequest;
import com.qubole.rubix.core.RemoteReadRequestChain;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import com.qubole.rubix.spi.thrift.BlockLocation;
import com.qubole.rubix.spi.thrift.BookKeeperService;
import com.qubole.rubix.spi.thrift.CacheStatusRequest;
import com.qubole.rubix.spi.thrift.ClusterNode;
import com.qubole.rubix.spi.thrift.FileInfo;
import com.qubole.rubix.spi.thrift.Location;
import com.qubole.rubix.spi.thrift.ReadDataRequest;
import com.qubole.rubix.spi.thrift.SetCachedRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.shaded.TException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

/**
 * Created by stagra on 12/2/16.
 */
public abstract class BookKeeper implements BookKeeperService.Iface
{
  private static Log log = LogFactory.getLog(BookKeeper.class);

  protected static Cache<String, FileMetadata> fileMetadataCache;
  private static LoadingCache<String, FileInfo> fileInfoCache;
  String nodeName;
  protected final Configuration conf;

  static long splitSize;
  private RemoteFetchProcessor fetchProcessor;
  private final Ticker ticker;
  private static long totalAvailableForCache;

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

  public BookKeeper(Configuration conf, BookKeeperMetrics bookKeeperMetrics) throws BookKeeperInitializationException
  {
    this(conf, bookKeeperMetrics, Ticker.systemTicker());
  }

  @Override
  public boolean isBookKeeperAlive()
  {
    return true;
  }

  @VisibleForTesting
  BookKeeper(Configuration conf, BookKeeperMetrics bookKeeperMetrics, Ticker ticker) throws BookKeeperInitializationException
  {
    this.conf = conf;
    this.bookKeeperMetrics = bookKeeperMetrics;
    this.metrics = bookKeeperMetrics.getMetricsRegistry();
    this.ticker = ticker;
    this.splitSize = CacheConfig.getCacheFileSplitSize(conf);
    cleanupOldCacheFiles(conf);

    setupCacheDirectory(conf);
    initializeMetrics();
    initializeCache(conf, ticker);
    fetchProcessor = null;
    if (CacheConfig.isParallelWarmupEnabled(conf)) {
      fetchProcessor = new RemoteFetchProcessor(this, metrics, conf);
    }
  }

  private void setupCacheDirectory(Configuration conf) throws BookKeeperInitializationException
  {
    try {
      CacheUtil.createCacheDirectories(conf);
    }
    catch (FileNotFoundException ex) {
      throw new BookKeeperInitializationException(ex.toString(), ex);
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
        }
      }
      catch (IOException ex) {
        log.error("Could not clean up the old cached files");
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
        return ((double) cacheRequestCount.getCount() / (cacheRequestCount.getCount() + remoteRequestCount.getCount()));
      }
    });
    metrics.register(CACHE_MISS_RATE_GAUGE.getMetricName(), new Gauge<Double>()
    {
      @Override
      public Double getValue()
      {
        return ((double) remoteRequestCount.getCount() / (cacheRequestCount.getCount() + remoteRequestCount.getCount()));
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
        return totalAvailableForCache;
      }
    });
  }

  @Override
  public List<BlockLocation> getCacheStatus(CacheStatusRequest request) throws TException
  {
    Map<Long, String> blockSplits = new HashMap<>();
    long blockNumber = 0;

    long fileLength = request.getFileLength();
    String remotePath = request.getRemotePath();
    long lastModified = request.getLastModified();
    long startBlock = request.getStartBlock();
    long endBlock = request.getEndBlock();

    for (long i = 0; i < fileLength; i = i + splitSize) {
      long end = i + splitSize;
      if (end > fileLength) {
        end = fileLength;
      }
      String key = remotePath + i + end;
      String hostName = getOwnerNodeForPath(key);
      blockSplits.put(blockNumber, hostName);
      blockNumber++;
    }

    FileMetadata md;
    try {
      md = fileMetadataCache.get(remotePath, new CreateFileMetadataCallable(remotePath, fileLength, lastModified, 0, conf));
      if (isInvalidationRequired(md.getLastModified(), lastModified)) {
        invalidateFileMetadata(remotePath);
        md = fileMetadataCache.get(remotePath, new CreateFileMetadataCallable(remotePath, fileLength, lastModified, 0, conf));
      }
    }
    catch (ExecutionException e) {
      log.error(String.format("Could not fetch Metadata for %s : %s", remotePath, Throwables.getStackTraceAsString(e)));
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
        if (blockSplits.get(split) == null) {
          blockLocations.add(new BlockLocation(Location.UNKNOWN, ""));
        }
        else if (!blockSplits.get(split).equalsIgnoreCase(nodeName)) {
          blockLocations.add(new BlockLocation(Location.NON_LOCAL, blockSplits.get(split)));
          nonLocalRequests++;
        }
        else {
          if (md.isBlockCached(blockNum)) {
            blockLocations.add(new BlockLocation(Location.CACHED, blockSplits.get(split)));
            cacheRequests++;
          }
          else {
            blockLocations.add(new BlockLocation(Location.LOCAL, blockSplits.get(split)));
            remoteRequests++;
          }
        }
      }
    }
    catch (IOException e) {
      throw new TException(e);
    }

    if (request.isIncrMetrics() && !isValidatingCachingBehavior(remotePath, conf)) {
      totalRequestCount.inc(totalRequests);
      nonlocalRequestCount.inc(nonLocalRequests);
      cacheRequestCount.inc(cacheRequests);
      remoteRequestCount.inc(remoteRequests);
    }

    return blockLocations;
  }

  @Override
  public void setAllCached(SetCachedRequest request)
      throws TException
  {
    FileMetadata md;
    md = fileMetadataCache.getIfPresent(request.getRemotePath());

    //md will be null when 2 users try to update the file in parallel and both their entries are invalidated.
    // TODO: find a way to optimize this so that the file doesn't have to be read again in next request (new data is stored instead of invalidation)
    if (md == null) {
      log.info(String.format("Could not update the metadata for file %s", request.getRemotePath()));
      return;
    }
    if (isInvalidationRequired(md.getLastModified(), request.getLastModified())) {
      invalidateFileMetadata(request.getRemotePath());
      return;
    }
    long endBlock = setCorrectEndBlock(request.getEndBlock(), request.getFileSize(), request.getRemotePath());
    log.debug("Updating cache for " + request.getRemotePath() + " StarBlock : " + request.getStartBlock() + " EndBlock : " + endBlock);

    try {
      md.setBlocksCached(request.getStartBlock(), endBlock);
      long currentFileSize = md.incrementCurrentFileSize((endBlock - request.getStartBlock()) * CacheConfig.getBlockSize(conf));
      replaceFileMetadata(request.getRemotePath(), currentFileSize, conf);
    }
    catch (IOException e) {
      throw new TException(e);
    }
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

  //This method is to ensure that data required by another node is cached before it is read by that node
  //using localTransferServer.
  // If the data is not already cached, remoteReadRequest for that block is sent and data is cached.

  //TODO : buffer is initialized everytime readData is called and it contains garbage value which is not required.
  // We cannot make it static because the variable is used in remoteReadRequestChain to write (cache) data to files.
  // So, it has to store the correct value in each thread. getCacheStatus is called twice.
  @Override
  public boolean readData(ReadDataRequest readDataRequest)
      throws TException
  {
    if (CacheConfig.isParallelWarmupEnabled(conf)) {
      startRemoteFetchProcessor();
      log.info("Adding to the queue Path : " + readDataRequest.getRemotePath() +
          " Offset : " + readDataRequest.getReadStart() + " Length " + readDataRequest.getReadLength());
      fetchProcessor.addToProcessQueue(readDataRequest.getRemotePath(), readDataRequest.getReadStart(),
          (int) readDataRequest.getReadLength(), readDataRequest.getFileSize(), readDataRequest.getLastModified());
      return true;
    }
    else {
      return readDataInternal(readDataRequest);
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
    if (CacheConfig.isFileStalenessCheckEnabled(conf)) {
      try {
        Path path = new Path(remotePath);
        FileSystem fs = path.getFileSystem(conf);
        FileStatus status = fs.getFileStatus(path);
        FileInfo info = new FileInfo(status.getLen(), status.getModificationTime());
        return info;
      }
      catch (Exception e) {
        log.error(String.format("Could not fetch FileStatus from remote file system for %s : %s", remotePath, Throwables.getStackTraceAsString(e)));
      }
    }
    else {
      try {
        return fileInfoCache.get(remotePath);
      }
      catch (ExecutionException e) {
        log.error(String.format("Could not fetch FileInfo from Cache for %s : %s", remotePath, Throwables.getStackTraceAsString(e)));
        throw new TException(e);
      }
    }

    return null;
  }

  public abstract List<ClusterNode> getClusterNodes();

  @Override
  public String getOwnerNodeForPath(String remotePathKey)
  {
    List<ClusterNode> nodeList = getClusterNodes();
    if (nodeList == null || nodeList.isEmpty()) {
      return null;
    }

    String hostName = ConsistentHashUtil.getHashedNodeForKey(nodeList, remotePathKey);
    return hostName;
  }

  private boolean readDataInternal(ReadDataRequest readDataRequest) throws TException
  {
    int blockSize = CacheConfig.getBlockSize(conf);
    byte[] buffer = new byte[blockSize];
    ByteBuffer byteBuffer = null;

    String remotePath = readDataRequest.getRemotePath();
    long offset = readDataRequest.getReadStart();
    long length = readDataRequest.getReadLength();
    long fileSize = readDataRequest.getFileSize();
    long lastModified = readDataRequest.getLastModified();

    String localPath = CacheUtil.getLocalPath(remotePath, conf);
    FileSystem fs = null;
    FSDataInputStream inputStream = null;
    Path path = new Path(remotePath);
    long startBlock = offset / blockSize;
    long endBlock = ((offset + (length - 1)) / CacheConfig.getBlockSize(conf)) + 1;
    try {
      int idx = 0;
      CacheStatusRequest request = new CacheStatusRequest(remotePath, fileSize, lastModified, startBlock, endBlock);
      List<BlockLocation> blockLocations = getCacheStatus(request);

      for (long blockNum = startBlock; blockNum < endBlock; blockNum++, idx++) {
        long readStart = blockNum * blockSize;
        log.debug(" blockLocation is: " + blockLocations.get(idx).getLocation() + " for path " + remotePath + " offset " + offset + " length " + length);
        if (blockLocations.get(idx).getLocation() != Location.CACHED) {
          if (byteBuffer == null) {
            byteBuffer = ByteBuffer.allocateDirect(CacheConfig.getDiskReadBufferSize(conf));
          }

          if (fs == null) {
            fs = path.getFileSystem(conf);
            log.info("Initializing FileSystem " + fs.toString() + " for Path " + path.toString());
            fs.initialize(path.toUri(), conf);

            inputStream = fs.open(path, blockSize);
          }

          // Cache the data
          // Ue RRRC directly instead of creating instance of CachingFS as in certain circumstances, CachingFS could
          // send this request to NonLocalRRC which would be wrong as that would not cache it on disk
          long expectedBytesToRead = (readStart + blockSize) > fileSize ? (fileSize - readStart) : blockSize;
          RemoteReadRequestChain remoteReadRequestChain = new RemoteReadRequestChain(inputStream, localPath, byteBuffer, buffer, new BookKeeperFactory(this));
          remoteReadRequestChain.addReadRequest(new ReadRequest(readStart, readStart + blockSize, readStart, readStart + blockSize, buffer, 0, fileSize));
          remoteReadRequestChain.lock();
          Integer dataRead = remoteReadRequestChain.call();
          // Making sure the data downloaded matches with the expected bytes. If not, there is some problem with
          // the download this time. So won't update the cache metadata and return false so that client can
          // fall back on the directread
          if (dataRead == expectedBytesToRead) {
            remoteReadRequestChain.updateCacheStatus(remotePath, fileSize, lastModified, blockSize, conf);
          }
          else {
            log.error("Not able to download requested bytes. Not updating the cache for block " + blockNum);
            return false;
          }
        }
      }
      return true;
    }
    catch (Exception e) {
      log.warn("Could not cache data: " + Throwables.getStackTraceAsString(e));
      return false;
    }
    finally {
      if (inputStream != null) {
        try {
          inputStream.close();
          fs.close();
        }
        catch (IOException e) {
          log.error(Throwables.getStackTraceAsString(e));
        }
      }
    }
  }

  private long setCorrectEndBlock(long endBlock, long fileLength, String remotePath)
  {
    long lastBlock = (fileLength - 1) / CacheConfig.getBlockSize(conf);
    if (endBlock > (lastBlock + 1)) {
      log.debug(String.format("Correct endBlock from %d to %d for path %s and length %d", endBlock, lastBlock + 1, remotePath, fileLength));
      endBlock = lastBlock + 1;
    }

    return endBlock;
  }

  private static synchronized void initializeCache(final Configuration conf, final Ticker ticker)
  {
    long avail = 0;
    for (int d = 0; d < CacheUtil.getCacheDiskCount(conf); d++) {
      avail += new File(CacheUtil.getDirPath(d, conf)).getUsableSpace();
    }
    avail = DiskUtils.bytesToMB(avail);
    log.info("total free space " + avail + "MB");

    // In corner cases evictions might not make enough space for new entries
    // To minimize those cases, consider available space lower than actual
    final long total = (long) (0.95 * avail);

    final long cacheMaxSize = CacheConfig.getCacheDataFullnessMaxSize(conf);
    totalAvailableForCache = (cacheMaxSize == 0)
        ? (long) (total * 1.0 * CacheConfig.getCacheDataFullnessPercentage(conf) / 100.0)
        : cacheMaxSize;

    initializeFileInfoCache(conf, ticker);

    fileMetadataCache = CacheBuilder.newBuilder()
        .ticker(ticker)
        .weigher(new Weigher<String, FileMetadata>()
        {
          @Override
          public int weigh(String key, FileMetadata md)
          {
            return md.getWeight(conf);
          }
        })
        .maximumWeight(totalAvailableForCache)
        .expireAfterWrite(CacheConfig.getCacheDataExpirationAfterWrite(conf), TimeUnit.MILLISECONDS)
        .removalListener(new CacheRemovalListener(conf))
        .build();
  }

  public FileMetadata getEntry(String key, Callable<FileMetadata> callable) throws ExecutionException
  {
    return fileMetadataCache.get(key, callable);
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
            log.info("Removed FileInfo for path " + notification.getKey() + " due to " + notification.getCause());
          }
        })
        .build(CacheLoader.asyncReloading(new CacheLoader<String, FileInfo>()
        {
          @Override
          public FileInfo load(String s) throws Exception
          {
            log.info("Fetching FileStatus for : " + s);
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
    final Configuration conf;
    CacheRemovalListener(Configuration configuration)
    {
      super();
      conf = configuration;
    }
    @Override
    public void onRemoval(RemovalNotification<String, FileMetadata> notification)
    {
      FileMetadata md = notification.getValue();
      try {
        md.closeAndCleanup(notification.getCause(), fileMetadataCache);
        if (!isValidatingCachingBehavior(md.getRemotePath(), conf)) {
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
      catch (IOException e) {
        log.warn("Could not cleanup FileMetadata for " + notification.getKey(), e);
      }
    }
  }

  private static class CreateFileMetadataCallable
      implements Callable<FileMetadata>
  {
    String path;
    Configuration conf;
    long fileLength;
    long lastModified;
    long currentFileSize;

    public CreateFileMetadataCallable(String path, long fileLength, long lastModified, long currentFileSize,
                                      Configuration conf)
    {
      this.path = path;
      this.conf = conf;
      this.fileLength = fileLength;
      this.lastModified = lastModified;
      this.currentFileSize = currentFileSize;
    }

    public FileMetadata call()
        throws Exception
    {
      return new FileMetadata(path, fileLength, lastModified, currentFileSize, conf);
    }
  }

  // This method is to invalidate FileMetadata from guava cache.
  @Override
  public void invalidateFileMetadata(String key)
  {
    // We might come in here with cache not initialized e.g. fs.create
    if (fileMetadataCache != null) {
      FileMetadata metadata = fileMetadataCache.getIfPresent(key);
      if (metadata != null) {
        log.info("Invalidating file " + key + " from metadata cache");
        fileMetadataCache.invalidate(key);
      }
    }
  }

  private void replaceFileMetadata(String key, long currentFileSize, Configuration conf) throws IOException
  {
    if (fileMetadataCache != null) {
      FileMetadata metadata = fileMetadataCache.getIfPresent(key);
      if (metadata != null) {
        FileMetadata newMetaData = new FileMetadata(key, metadata.getFileSize(), metadata.getLastModified(),
            currentFileSize, conf);
        fileMetadataCache.put(key, newMetaData);
      }
    }
  }

  private boolean isInvalidationRequired(long metadataLastModifiedTime, long remoteLastModifiedTime)
  {
    if (CacheConfig.isFileStalenessCheckEnabled(conf) && (metadataLastModifiedTime != remoteLastModifiedTime)) {
      return true;
    }

    return false;
  }

  private static boolean isValidatingCachingBehavior(String remotePath, Configuration conf)
  {
    return CachingValidator.VALIDATOR_TEST_FILE_NAME.equals(CacheUtil.getName(remotePath, conf));
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
