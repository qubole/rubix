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
package com.qubole.rubix.client.robotframework.container.client;

import com.qubole.rubix.client.robotframework.container.server.RequestServer;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;

public class ContainerRequestClient
{
  private static final String REQUEST_SERVER_NAME = "ContainerRequestServer";

  public ContainerRequestClient()
  {
  }

  /**
   * Get the current cache metrics from the BookKeeper server on a particular node.
   *
   * @param port  The port on which the desired request server is exposed.
   * @return A map of metrics describing cache statistics and interactions for that node.
   */
  public Map<String, Double> getCacheMetricsForNode(int port)
  {
    try {
      final RequestServer containerServer = getRequestServer(port);
      return containerServer.getCacheMetrics(new GetCacheMetricsRequest());
    }
    catch (RemoteException | NotBoundException e) {
      System.err.println("ContainerRequestClient exception:");
      e.printStackTrace();
    }
    return new HashMap<>();
  }

  /**
   * Read data from a given file into the BookKeeper cache using a client caching file system.
   *
   * @param port  The port on which the desired request server is exposed.
   * @param remotePath  The remote path location.
   * @param readStart  The block to start reading from.
   * @param length  The amount of data to read.
   * @param fileSize  The length of the file.
   * @param lastModified  The time at which the file was last modified.
   * @param clusterType  The type id of cluster being used.
   * @return True if the data was read into the cache correctly, false otherwise.
   */
  public boolean cacheDataUsingClientFileSystemForNode(
      int port,
      String remotePath,
      long readStart,
      int length,
      long fileSize,
      long lastModified,
      int clusterType)
  {
    ReadDataRequestParams params = new ReadDataRequestParams(remotePath, readStart, length, fileSize, lastModified, clusterType);

    try {
      RequestServer containerServer = getRequestServer(port);
      return containerServer.cacheDataUsingClientFileSystem(new ReadDataWithFileSystemRequest(), params);
    }
    catch (RemoteException | NotBoundException e) {
      System.err.println("ContainerRequestClient exception:");
      e.printStackTrace();
    }
    return false;
  }

  /**
   * Locates a {@link RequestServer} for executing requests on a particular container.
   *
   * @param port  The port on which the desired request server is exposed.
   * @return The {@link RequestServer} used for executing requests.
   * @throws RemoteException if the registry could not be located or communicated with.
   * @throws NotBoundException if the registry has not been bould.
   */
  private static RequestServer getRequestServer(int port) throws RemoteException, NotBoundException
  {
    Registry registry = LocateRegistry.getRegistry("127.0.0.1", port);
    return (RequestServer) registry.lookup(REQUEST_SERVER_NAME);
  }
}
