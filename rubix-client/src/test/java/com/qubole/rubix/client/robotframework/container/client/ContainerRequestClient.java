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

import com.qubole.rubix.client.robotframework.TestClientReadRequest;
import com.qubole.rubix.client.robotframework.testdriver.CoordinatorRemote;
import com.qubole.rubix.client.robotframework.testdriver.CoordinatorTestDriver;
import com.qubole.rubix.client.robotframework.testdriver.Job;
import com.qubole.rubix.client.robotframework.testdriver.Task;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;

public class ContainerRequestClient
{
  private static final Log log = LogFactory.getLog(ContainerRequestClient.class);

  private static final int REGISTRY_PORT = 1099;
  private static final String FILE_SCHEME = "file:";

  public ContainerRequestClient()
  {
  }

  public Job makeJob(
      int cacheRequestRatio,
      int remoteRequestRatio,
      int nonLocalRequestRatio,
      List<String> fileNames,
      int startBlock,
      int endBlock,
      long fileLength,
      long lastModified,
      int clusterType)
  {
    List<Task> tasks = new ArrayList<>();
    for (String fileName : fileNames) {
      tasks.add(new Task(new TestClientReadRequest(fileName, startBlock, endBlock, fileLength, lastModified, clusterType)));
    }
    return new Job(tasks, cacheRequestRatio, remoteRequestRatio, nonLocalRequestRatio);
  }

  public boolean runRubixJob(String host, Job job)
  {
    try {
      final CoordinatorRemote coordinator = getDriverServer(host);
      return coordinator.executeJob(job);
    }
    catch (RemoteException | NotBoundException e) {
      System.err.println("ContainerRequestClient exception:");
      e.printStackTrace();
    }
    return false;
  }

  public boolean verifyRubixJob(String host, Job job)
  {
    try {
      final CoordinatorRemote coordinator = getDriverServer(host);
      return coordinator.verifyJob(job);
    }
    catch (RemoteException | NotBoundException e) {
      System.err.println("ContainerRequestClient exception:");
      e.printStackTrace();
    }
    return false;
  }

  private static CoordinatorRemote getDriverServer(String host) throws RemoteException, NotBoundException
  {
    Registry registry = LocateRegistry.getRegistry(host, REGISTRY_PORT);
    return (CoordinatorRemote) registry.lookup(CoordinatorTestDriver.SERVER_NAME);
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
