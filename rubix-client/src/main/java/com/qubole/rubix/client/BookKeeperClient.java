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
package com.qubole.rubix.client;

import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.TException;
import org.apache.thrift.shaded.transport.TTransportException;

import java.io.IOException;
import java.util.Map;

public class BookKeeperClient
{
  private final BookKeeperFactory factory = new BookKeeperFactory();
  private final Configuration conf = new Configuration();

  /**
   * Read data from a given file into the BookKeeper cache.
   *
   * @param remotePath    The remote path location.
   * @param readStart     The block to start reading from.
   * @param readLength    The amount
   * @param fileLength    The length of the file.
   * @param lastModified  The time at which the file was last modified.
   * @param clusterType   The type id of cluster being used.
   * @return True if the data was read into the cache correctly, false
   */
  public boolean readData(String remotePath, long readStart, int readLength, long fileLength, long lastModified, int clusterType) throws IOException, TException
  {
    try (RetryingBookkeeperClient client = createBookKeeperClient()) {
      return client.readData(remotePath, readStart, readLength, fileLength, lastModified, clusterType);
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
   * Create a client for interacting to a BookKeeper server.
   *
   * @return The BookKeeper client.
   * @throws TTransportException if an error occurs when trying to connect to the BookKeeper server.
   */
  private RetryingBookkeeperClient createBookKeeperClient() throws TTransportException
  {
    return factory.createBookKeeperClient(conf);
  }
}
