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
import java.util.Map;
import java.util.Random;

public class BookKeeperClientRFLibrary
{
  private final BookKeeperFactory factory = new BookKeeperFactory();
  private final Configuration conf = new Configuration();

  /**
   * Read data from a given file into the BookKeeper cache using the BookKeeper Thrift API.
   *
   * @param remotePath    The remote path location.
   * @param readStart     The block to start reading from.
   * @param readLength    The amount of data to read.
   * @param fileLength    The length of the file.
   * @param lastModified  The time at which the file was last modified.
   * @param clusterType   The type id of cluster being used.
   * @return True if the data was read into the cache correctly, false otherwise.
   */
  public boolean downloadDataToCache(String remotePath, long readStart, int readLength, long fileLength, long lastModified, int clusterType) throws IOException, TException
  {
    try (RetryingBookkeeperClient client = createBookKeeperClient()) {
      return client.readData(remotePath, readStart, readLength, fileLength, lastModified, clusterType);
    }
  }

  /**
   * Read data from a given file into the BookKeeper cache using a client caching file system.
   *
   * @param remotePath    The remote path location.
   * @param readStart     The block to start reading from.
   * @param readLength    The amount of data to read.
   * @return True if the data was read into the cache correctly, false otherwise.
   */
  public boolean readData(String remotePath, long readStart, int readLength) throws IOException, TException, URISyntaxException
  {
    try (FSDataInputStream inputStream = createFSInputStream(remotePath, readLength)) {
      final int readSize = inputStream.read(new byte[readLength], (int) readStart, readLength);
      return readSize == readLength;
    }
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
    mockFS.initialize(URI.create("file://" + remotePath), conf);
    return mockFS.open(new Path(remotePath), readLength);
  }
}
