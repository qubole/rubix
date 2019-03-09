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
package com.qubole.rubix.client.robotframework;

import com.qubole.rubix.core.MockCachingFileSystem;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
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

  /**
   * Read data from a given file into the BookKeeper cache using the BookKeeper Thrift API.
   *
   * @param readRequest The read request to execute.
   * @return True if the data was read into the cache correctly, false otherwise.
   */
  public boolean downloadDataToCache(TestClientReadRequest readRequest) throws IOException, TException
  {
    try (RetryingBookkeeperClient client = createBookKeeperClient()) {
      return client.readData(
          readRequest.getRemotePath(),
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
  public boolean multiDownloadDataToCache(int numThreads,
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
          return downloadDataToCache(request);
        }
      };
      tasks.add(callable);
    }

    final List<Future<Boolean>> results = executeMultipleTasks(numThreads, tasks, staggerRequests);
    final boolean didAllSucceed = verifyReadRequests(results);
    return didAllSucceed;
  }

  /**
   * Read data from a given file into the BookKeeper cache using a client caching file system.
   *
   * @param readRequest The read request to execute.
   * @return True if the data was read into the cache correctly, false otherwise.
   */
  public boolean readData(TestClientReadRequest readRequest) throws IOException, TException, URISyntaxException
  {
    try (FSDataInputStream inputStream = createFSInputStream(readRequest.getRemotePath(), readRequest.getReadLength())) {
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
  public boolean multiReadData(int numThreads,
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
          return readData(request);
        }
      };
      tasks.add(callable);
    }

    final List<Future<Boolean>> results = executeMultipleTasks(numThreads, tasks, staggerRequests);
    final boolean didAllSucceed = verifyReadRequests(results);
    return didAllSucceed;
  }

  /**
   * Get the current cache metrics from the BookKeeper server.
   *
   * @return A map of metrics describing cache statistics and interactions.
   */
  public Map<String, Double> getCacheMetrics() throws IOException, TException
  {
    try (RetryingBookkeeperClient client = createBookKeeperClient()) {
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
  private RetryingBookkeeperClient createBookKeeperClient() throws TTransportException
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
  private <T> List<Future<T>> executeMultipleTasks(int numThreads, List<Callable<T>> tasks, boolean staggerTasks) throws InterruptedException
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
  private boolean verifyReadRequests(List<Future<Boolean>> readRequestResults) throws ExecutionException, InterruptedException
  {
    boolean didAllSucceed = true;
    for (final Future<Boolean> downloadDataResult : readRequestResults) {
      final Boolean result = downloadDataResult.get();
      didAllSucceed &= result;
    }
    return didAllSucceed;
  }
}
