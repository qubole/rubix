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
package com.qubole.rubix.client.robotframework;

import com.qubole.rubix.core.MockCachingFileSystem;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheUtil;
import com.qubole.rubix.spi.RetryingPooledBookkeeperClient;
import com.qubole.rubix.spi.thrift.BlockLocation;
import com.qubole.rubix.spi.thrift.CacheStatusRequest;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.shaded.TException;
import org.apache.thrift.shaded.transport.TTransportException;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class BookKeeperClientRFLibrary
{
  private final BookKeeperFactory factory = new BookKeeperFactory();
  private final Configuration conf = new Configuration();
  private static final String FILE_SCHEME = "file:";

  /**
   * Read data from a given file into the BookKeeper cache using the BookKeeper Thrift API.
   *
   * @param readRequest The read request to execute.
   * @return True if the data was read into the cache correctly, false otherwise.
   */
  public boolean cacheDataUsingBookKeeperServerCall(TestClientReadRequest readRequest) throws IOException, TException
  {
    try (RetryingPooledBookkeeperClient client = createBookKeeperClient()) {
      return client.readData(
          getPathWithFileScheme(readRequest.getRemotePath()),
          readRequest.getReadStart(),
          readRequest.getReadLength(),
          readRequest.getFileLength(),
          readRequest.getLastModified(),
          readRequest.getClusterType());
    }
  }

  /**
   * Read data into the BookKeeper cache from multiple concurrent clients using the BookKeeper Thrift API.
   *
   * @param numThreads    The number of threads to use for concurrent execution.
   * @param readRequests  The read requests to concurrently execute.
   * @return True if all read requests succeeded, false otherwise.
   */
  public boolean concurrentlyCacheDataUsingBookKeeperServerCall(int numThreads,
                                                                boolean staggerRequests,
                                                                List<TestClientReadRequest> readRequests) throws TException, InterruptedException, ExecutionException
  {
    final List<Callable<Boolean>> tasks = new ArrayList<>();
    for (final TestClientReadRequest request : readRequests) {
      final Callable<Boolean> callable = new Callable<Boolean>()
      {
        @Override
        public Boolean call() throws Exception
        {
          return cacheDataUsingBookKeeperServerCall(request);
        }
      };
      tasks.add(callable);
    }

    final List<Future<Boolean>> results = executeConcurrentTasks(numThreads, tasks, staggerRequests);
    final boolean didAllSucceed = didConcurrentDataDownloadSucceed(results);
    return didAllSucceed;
  }

  /**
   * Read data from a given file into the BookKeeper cache using a client caching file system.
   *
   * @param readRequest The read request to execute.
   * @return True if the data was read into the cache correctly, false otherwise.
   */
  public boolean cacheDataUsingClientFileSystem(TestClientReadRequest readRequest) throws IOException, TException, URISyntaxException
  {
    try (FSDataInputStream inputStream = createFSInputStream(getPathWithFileScheme(readRequest.getRemotePath()), readRequest.getReadLength())) {
      final int readSize = inputStream.read(
          new byte[readRequest.getReadLength()],
          (int) readRequest.getReadStart(),
          readRequest.getReadLength());
      return readSize == readRequest.getReadLength();
    }
  }

  /**
   * Read data into the BookKeeper cache from multiple concurrent clients using a client caching file system.
   *
   * @param numThreads    The number of threads to use for concurrent execution.
   * @param readRequests  The read requests to concurrently execute.
   * @return True if all read requests succeeded, false otherwise.
   */
  public boolean concurrentlyCacheDataUsingClientFileSystem(int numThreads,
                                                            boolean staggerRequests,
                                                            List<TestClientReadRequest> readRequests) throws TException, InterruptedException, ExecutionException
  {
    final List<Callable<Boolean>> tasks = new ArrayList<>();
    for (final TestClientReadRequest request : readRequests) {
      final Callable<Boolean> callable = new Callable<Boolean>()
      {
        @Override
        public Boolean call() throws Exception
        {
          return cacheDataUsingClientFileSystem(request);
        }
      };
      tasks.add(callable);
    }

    final List<Future<Boolean>> results = executeConcurrentTasks(numThreads, tasks, staggerRequests);
    final boolean didAllSucceed = didConcurrentDataDownloadSucceed(results);
    return didAllSucceed;
  }

  /**
   * Get the cache status for blocks in a particular file.
   *
   * @param request The request specifying the blocks & file for which to check the status.
   * @return A list of {@link BlockLocation}s detailing the locations of the specified blocks.
   */
  public List<BlockLocation> getCacheStatus(TestClientStatusRequest request) throws IOException, TException
  {
    try (RetryingPooledBookkeeperClient client = createBookKeeperClient()) {
      return client.getCacheStatus(new CacheStatusRequest(
          getPathWithFileScheme(request.getRemotePath()),
          request.getFileLength(),
          request.getLastModified(),
          request.getStartBlock(),
          request.getEndBlock(),
          request.getClusterType()));
    }
  }

  /**
   * Get the current cache metrics from the BookKeeper server.
   *
   * @return A map of metrics describing cache statistics and interactions.
   */
  public Map<String, Double> getCacheMetrics() throws IOException, TException
  {
    try (RetryingPooledBookkeeperClient client = createBookKeeperClient()) {
      return client.getCacheMetrics();
    }
  }

  /**
   * Get the combined size of all configured cache directories.
   *
   * @param dirPath     The root path for the cache directories
   * @param dirSuffix   The cache directory suffix.
   * @param numDisks    The expected number of cache disks.
   * @return The size of the cache in MB.
   */
  public long getCacheDirSizeMb(String dirPath, String dirSuffix, int numDisks)
  {
    long cacheSize = 0;
    for (int disk = 0; disk < numDisks; disk++) {
      long cacheDirSize = FileUtils.sizeOfDirectory(new File(dirPath + disk + dirSuffix));
      cacheSize += cacheDirSize;
    }

    return (cacheSize / 1024 / 1024);
  }

  /**
   * Generate a file to be used during testing.
   *
   * @param filename  The name of the file.
   * @param size      The size of the file in bytes.
   * @throws IOException if an error occurs while writing to the specified file.
   */
  public void generateTestFile(String filename, long size) throws IOException
  {
    String content = generateContent(size);
    Files.write(Paths.get(filename), content.getBytes());
  }

  /**
   * Generate a metadata file to be used for testing situations where metadata exists without its matching cache file.
   *
   * @param filename  The name of the file.
   * @throws IOException if an error occurs while writing to the specified file.
   */
  public String generateTestMDFile(String filename) throws IOException
  {
    String mdPath = CacheUtil.getMetadataFilePath(filename, conf);
    // Certain tests require a non-empty metadata file.
    Files.write(Paths.get(mdPath), "0101010101".getBytes());
    return mdPath;
  }

  /**
   * Create a read request to be executed by the BookKeeper server.
   *
   * @param remotePath    The remote path location.
   * @param readStart     The block to start reading from.
   * @param readLength    The amount of data to read.
   * @param fileLength    The length of the file.
   * @param lastModified  The time at which the file was last modified.
   * @param clusterType   The type id of cluster being used.
   * @return The read request.
   */
  public TestClientReadRequest createTestClientReadRequest(String remotePath,
                                                           long readStart,
                                                           int readLength,
                                                           long fileLength,
                                                           long lastModified,
                                                           int clusterType)
  {
    return new TestClientReadRequest(remotePath, readStart, readLength, fileLength, lastModified, clusterType);
  }

  /**
   * Create a status request to be executed by the BookKeeper server.
   *
   * @param remotePath    The remote path location.
   * @param fileLength    The length of the file.
   * @param lastModified  The time at which the file was last modified.
   * @param startBlock    The start of the block range to check.
   * @param endBlock      The end of the block range to check.
   * @param clusterType   The type id of cluster being used.
   * @return The status request.
   */
  public TestClientStatusRequest createTestClientStatusRequest(String remotePath,
                                                               long fileLength,
                                                               long lastModified,
                                                               long startBlock,
                                                               long endBlock,
                                                               int clusterType)
  {
    return new TestClientStatusRequest(remotePath, fileLength, lastModified, startBlock, endBlock, clusterType);
  }

  /**
   * Generate a random character string.
   *
   * @param size  The size of the string to generate.
   * @return A random string of the specified size.
   */
  private String generateContent(long size)
  {
    StringBuilder builder = new StringBuilder();
    Random random = new Random();

    for (int i = 0; i < size; i++) {
      char randomChar = (char) (33 + random.nextInt(93));
      builder.append(randomChar);
    }
    return builder.toString();
  }

  /**
   * Initializes the Hadoop configuration for the library.
   *
   * @param configurationOptions The options to set.
   */
  public void initializeLibraryConfiguration(Map<String, String> configurationOptions)
  {
    conf.clear();
    for (final Map.Entry<String, String> option : configurationOptions.entrySet()) {
      conf.set(option.getKey(), option.getValue());
    }
  }

  /**
   * Create a client for interacting to a BookKeeper server.
   *
   * @return The BookKeeper client.
   * @throws TTransportException if an error occurs when trying to connect to the BookKeeper server.
   */
  private RetryingPooledBookkeeperClient createBookKeeperClient() throws TTransportException
  {
    return factory.createBookKeeperClient(conf);
  }

  /**
   * Creates & initializes an input stream for executing client caching.
   *
   * @param remotePath  The path of the file to cache.
   * @return the input stream for the file.
   * @throws IOException if an error occurs when initializing the file system.
   */
  private FSDataInputStream createFSInputStream(String remotePath, int readLength) throws IOException
  {
    final MockCachingFileSystem mockFS = new MockCachingFileSystem();
    mockFS.initialize(URI.create(remotePath), conf);
    return mockFS.open(new Path(remotePath), readLength);
  }

  /**
   * Execute multiple tasks concurrently.
   *
   * @param numThreads   The number of available threads for concurrent execution.
   * @param tasks        The tasks to execute.
   * @param staggerTasks If true, add delay between task submissions.
   * @param <T>          The return type of the task.
   * @return A list of results for each task executed.
   * @throws InterruptedException if task execution is interrupted.
   */
  private <T> List<Future<T>> executeConcurrentTasks(int numThreads, List<Callable<T>> tasks, boolean staggerTasks) throws InterruptedException
  {
    final ExecutorService service = Executors.newFixedThreadPool(numThreads);
    List<Future<T>> futures = new ArrayList<>();

    if (staggerTasks) {
      // Necessary to preserve order of requests for certain tests.
      for (Callable<T> task : tasks) {
        futures.add(service.submit(task));
        Thread.sleep(100);
      }
    }
    else {
      futures = service.invokeAll(tasks);
    }
    return futures;
  }

  /**
   * Verify the results of read requests executed concurrently.
   *
   * @param readRequestResults   The results to verify.
   * @return True if all read requests succeeded, false otherwise.
   */
  private boolean didConcurrentDataDownloadSucceed(List<Future<Boolean>> readRequestResults) throws ExecutionException, InterruptedException
  {
    boolean didAllSucceed = true;
    for (final Future<Boolean> result : readRequestResults) {
      final Boolean didRead = result.get();
      didAllSucceed &= didRead;
    }
    return didAllSucceed;
  }

  /**
   * Watch the cache directory for changes and verify state of cached files.
   *
   * @param cacheDir     The directory to watch.
   * @param maxWaitTime  The maximum amount of time to wait for file events.
   * @param requests     The read requests describing the files to be watched for.
   * @return True if all expected files have been
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public boolean watchCache(final String cacheDir, final int maxWaitTime, final List<TestClientReadRequest> requests) throws ExecutionException, InterruptedException
  {
    Future<Boolean> watcherResult = startCacheWatcher(cacheDir, requests, maxWaitTime);
    boolean didCache = watcherResult.get();
    return didCache;
  }

  /**
   * Watch the cache directory for changes and verify state of cached files.
   *
   * @param cacheDir     The directory to watch.
   * @param maxWaitTime  The maximum amount of time to wait for file events.
   * @param requests     The read requests describing the files to be watched for.
   * @return The {@link Future} for the result of the cache watcher.
   */
  private Future<Boolean> startCacheWatcher(final String cacheDir, final List<TestClientReadRequest> requests, final int maxWaitTime)
  {
    return Executors.newSingleThreadExecutor().submit(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        CacheWatcher watcher = new CacheWatcher(conf, Paths.get(cacheDir), maxWaitTime);
        return watcher.watchForCacheFiles(requests);
      }
    });
  }

  /**
   * Add the file scheme to the provided path for proper execution with the BookKeeper server.
   *
   * @param path  The path to update.
   * @return The provided path with the file scheme.
   */
  private String getPathWithFileScheme(String path)
  {
    return FILE_SCHEME + path;
  }
}
