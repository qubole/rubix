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
package com.qubole.rubix.client.robotframework.container.server;

import com.qubole.rubix.client.robotframework.container.client.GetCacheMetricsRequest;
import com.qubole.rubix.client.robotframework.container.client.ReadDataRequestParams;
import com.qubole.rubix.client.robotframework.container.client.ReadDataWithFileSystemRequest;
import com.qubole.rubix.core.MockCachingFileSystem;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.shaded.transport.TTransportException;

import java.io.IOException;
import java.net.URI;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;

public class ContainerRequestServer implements RequestServer
{
  private static final Log log = LogFactory.getLog(ContainerRequestServer.class);
  public static final int SERVER_PORT = 8123;

  private final BookKeeperFactory factory = new BookKeeperFactory();
  private final Configuration conf = new Configuration();

  public ContainerRequestServer()
  {
    super();
  }

  @Override
  public Map<String, Double> getCacheMetrics(GetCacheMetricsRequest request)
  {
    try (RetryingBookkeeperClient client = createBookKeeperClient()) {
      return request.execute(client);
    }
    catch (TTransportException e) {
      log.error("Error getting cache metrics", e);
    }
    return new HashMap<>();
  }

  @Override
  public boolean cacheDataUsingClientFileSystem(ReadDataWithFileSystemRequest request, ReadDataRequestParams params) throws RemoteException
  {
    try (FSDataInputStream inputStream = createFSInputStream(params.getRemotePath(), params.getLength())) {
      return request.execute(inputStream, params);
    }
    catch (IOException e) {
      log.error("Error caching data using CachingFileSystem", e);
    }
    return false;
  }

  /**
   * Create a client for interacting to a BookKeeper server.
   *
   * @return The BookKeeper client.
   * @throws TTransportException if an error occurs when trying to connect to the BookKeeper server.
   */
  private RetryingBookkeeperClient createBookKeeperClient()
          throws TTransportException
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
   * Binds an RMI server to the registry for executing RubiX requests.
   *
   * @throws RemoteException if the server could not be bound.
   */
  private static void bindServer() throws RemoteException
  {
    final RequestServer server = (RequestServer) UnicastRemoteObject.exportObject(new ContainerRequestServer(), SERVER_PORT);
    final Registry registry = LocateRegistry.getRegistry();
    registry.rebind("ContainerRequestServer", server);
  }

  public static void main(String[] args)
  {
    try {
      bindServer();
      log.debug("ContainerRequestServer bound");
    }
    catch (RemoteException e) {
      log.error("Error binding server", e);
    }
  }
}
