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
package com.qubole.rubix.client.robotframework.testdriver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class WorkerTestDriver implements WorkerRemote
{
  private static Log log = LogFactory.getLog(WorkerTestDriver.class);
  public static final String SERVER_NAME = "WorkerTestDriver";
  private static final int SERVER_PORT = 8564;

  @Override
  public boolean executeTask(Task task) throws RemoteException
  {
    return true;
  }

  /**
   * Binds an RMI server to the registry for executing RubiX requests.
   *
   * @throws RemoteException if the server could not be bound.
   */
  private static void bindServer() throws RemoteException
  {
    final WorkerRemote server = (WorkerRemote) UnicastRemoteObject.exportObject(new WorkerTestDriver(), SERVER_PORT);
    final Registry registry = LocateRegistry.getRegistry();
    registry.rebind(SERVER_NAME, server);
  }

  public static void main(String[] args)
  {
    try {
      bindServer();
      log.debug("WorkerTestDriver bound");
    }
    catch (RemoteException e) {
      log.error("Error binding server", e);
    }
  }
}
