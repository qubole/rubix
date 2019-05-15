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

import com.qubole.rubix.client.robotframework.driver.execute.RubiXRequest;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.transport.TTransportException;

import java.io.IOException;
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
  public <T> T executeRequest(RubiXRequest<T> request) throws RemoteException
  {
    T result = null;
    log.info("Received request");
    try (RetryingBookkeeperClient client = createBookKeeperClient()) {
      log.info("Executing request");
      result = request.execute(client);
    }
    catch (IOException | TTransportException e) {
      e.printStackTrace();
    }
    log.info("Returning result");
    return result;
  }

  private RetryingBookkeeperClient createBookKeeperClient() throws TTransportException
  {
    log.info("Creating BK client");
    return factory.createBookKeeperClient(conf);
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
