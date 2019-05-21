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
package com.qubole.rubix.client.robotframework.driver.server;

import com.qubole.rubix.client.robotframework.driver.client.ReadDataCFSRequest;
import com.qubole.rubix.client.robotframework.driver.client.ReadDataRequestParams;
import com.qubole.rubix.client.robotframework.driver.execute.RubiXRequest;
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
  public <T, P> T executeGetCacheMetricsRequest(RubiXRequest<T, P> request) throws RemoteException
  {
    T result = null;
    log.info("Received request");
    try (RetryingBookkeeperClient client = createBookKeeperClient()) {
      log.info("Executing request");
      result = request.execute(client, null);
    }
    catch (IOException | TTransportException e) {
      e.printStackTrace();
    }
    log.info("Returning result");
    return result;
  }

  @Override
  public <T, P> T executeReadDataRequest(RubiXRequest<T, P> request, P params) throws RemoteException
  {
    T result = null;
    try (RetryingBookkeeperClient client = createBookKeeperClient()) {
      result = request.execute(client, params);
    }
    catch (IOException | TTransportException e) {
      e.printStackTrace();
    }
    log.info("Returning rd result");
    return result;
  }

  @Override
  public boolean executeReadDataRequestWithCFS(ReadDataCFSRequest request, ReadDataRequestParams params) throws RemoteException
  {
    try (FSDataInputStream inputStream = createFSInputStream(params.getRemotePath(), params.getLength())) {
      boolean didRead = request.execute(inputStream, params);
      log.info("Returning rd-cfs result: " + didRead);
      return didRead;
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  private RetryingBookkeeperClient createBookKeeperClient() throws TTransportException
  {
    log.info("Creating BK client");
    return factory.createBookKeeperClient(conf);
  }

  private FSDataInputStream createFSInputStream(String remotePath, int readLength) throws IOException
  {
    final MockCachingFileSystem mockFS = new MockCachingFileSystem();
    mockFS.initialize(URI.create(remotePath), conf);
    return mockFS.open(new Path(remotePath), readLength);
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

  private static void bindServer() throws RemoteException
  {
    RequestServer engine = new ContainerRequestServer();
    RequestServer stub = (RequestServer) UnicastRemoteObject.exportObject(engine, SERVER_PORT);
    Registry registry = LocateRegistry.getRegistry();
    registry.rebind("ContainerRequestServer", stub);
  }
}
