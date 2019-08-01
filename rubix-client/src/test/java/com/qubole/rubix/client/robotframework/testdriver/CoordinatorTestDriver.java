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

import com.qubole.rubix.client.robotframework.container.server.RequestServer;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.TException;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CoordinatorTestDriver implements CoordinatorRemote
{
  private static Log log = LogFactory.getLog(CoordinatorTestDriver.class);
  public static final String SERVER_NAME = "CoordinatorTestDriver";
  private static final int MAX_THREADS = 10;
  private static final int SERVER_PORT = 8300;

  private static final String WORKER1_HOSTNAME = "172.18.8.1";

  private Map<String, WorkerRemote> workerDriverMap = new HashMap<>();
  private BookKeeperFactory factory = new BookKeeperFactory();
  private Configuration conf = new Configuration();

  // RMI server for RF: receive requests from test
  // RMI client for Worker: send tasks to worker node

  @Override
  public boolean executeJob(Job job) throws RemoteException
  {
    // getWorkerReferences_stub();
    getWorkerReferencesFromBKS();
    log.info("Number of workers: " + workerDriverMap.size());
    // get tasks from Job
    List<Task> tasks = job.getTasks();
    // schedule tasks
    Map<String, List<Task>> workerTaskMap = scheduleTasks(tasks);
    // execute tasks
    try {
      return executeWorkerTasks(workerTaskMap);
    }
    catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    return false;
  }

  private boolean executeWorkerTasks(Map<String, List<Task>> taskMap) throws InterruptedException, ExecutionException
  {
    final ExecutorService service = Executors.newFixedThreadPool(MAX_THREADS);
    final List<Callable<Boolean>> callables = new ArrayList<>();

    for (Map.Entry<String, List<Task>> tasks : taskMap.entrySet()) {
      final String workerHostname = tasks.getKey();
      List<Task> workerTasks = tasks.getValue();

      for (final Task task : workerTasks) {
        callables.add(new Callable<Boolean>()
        {
          @Override
          public Boolean call() throws Exception
          {
            WorkerRemote worker = workerDriverMap.get(workerHostname);
            return worker.executeTask(task);
          }
        });
      }
    }

    List<Future<Boolean>> taskResults = service.invokeAll(callables);
    boolean didAllSucceed = true;
    for (final Future<Boolean> result : taskResults) {
      final Boolean didRead = result.get();
      didAllSucceed &= didRead;
    }
    return didAllSucceed;
  }

  /**
   * Execute multiple tasks concurrently.
   *
   * @param numThreads   The number of available threads for concurrent execution.
   * @param tasks        The tasks to execute.
   * @param <T>          The return type of the task.
   * @return A list of results for each task executed.
   * @throws InterruptedException if task execution is interrupted.
   */
  private <T> List<Future<T>> executeConcurrentTasks(int numThreads, List<Callable<T>> tasks) throws InterruptedException
  {
    final ExecutorService service = Executors.newFixedThreadPool(numThreads);
    List<Future<T>> futures;

    futures = service.invokeAll(tasks);

    return futures;
  }

  private Map<String, List<Task>> scheduleTasks(List<Task> tasks)
  {
    Map<String, List<Task>> workerTaskMap = new HashMap<>();
    for (Task task : tasks) {
      List<Task> workerTasks = workerTaskMap.get(WORKER1_HOSTNAME);
      if (workerTasks == null) {
        workerTasks = new ArrayList<>();
        workerTaskMap.put(WORKER1_HOSTNAME, workerTasks);
      }
      workerTasks.add(task);
      // workerTaskMap.put(WORKER1_HOSTNAME, task);
      // Determine where files would norma
      // NONLOCAL: pick
      //
    }
    return workerTaskMap;
  }

  // TODO STUB until ready
  // private void getWorkerReferences_stub()
  // {
  //   String nodeHostname = WORKER1_HOSTNAME;
  //   WorkerRemote workerDriver = null;
  //   try {
  //     workerDriver = getWorkerTestDriverForNode(nodeHostname);
  //   }
  //   catch (RemoteException | NotBoundException e) {
  //     log.error("CoordinatorTestDriver exception", e);
  //   }
  //   workerDriverMap.put(nodeHostname, workerDriver);
  // }

  private void getWorkerReferencesFromBKS()
  {
    // get list of nodes from BK daemon
    try (RetryingBookkeeperClient client = factory.createBookKeeperClient(conf)) {
      List<String> nodes = client.getClusterNodes();

      for (String node : nodes) {
        WorkerRemote workerDriver = getWorkerTestDriverForNode(node);
        workerDriverMap.put(node, workerDriver);
      }
    }
    catch (TException | IOException e) {
      e.printStackTrace();
    }
    catch (NotBoundException e) {
      e.printStackTrace();
    }
  }

  private static void bindServer() throws RemoteException
  {
    final CoordinatorRemote server = (CoordinatorRemote) UnicastRemoteObject.exportObject(new CoordinatorTestDriver(), SERVER_PORT);
    final Registry registry = LocateRegistry.getRegistry();
    registry.rebind(CoordinatorTestDriver.SERVER_NAME, server);
  }

  /**
   * Locates a {@link RequestServer} for executing requests on a particular container.
   *
   * @param host  The hostname for the container to connect to.
   * @return The {@link RequestServer} used for executing requests.
   * @throws RemoteException if the registry could not be located or communicated with.
   * @throws NotBoundException if the registry has not been bould.
   */
  private static WorkerRemote getWorkerTestDriverForNode(String host) throws RemoteException, NotBoundException
  {
    Registry registry = LocateRegistry.getRegistry(host, Registry.REGISTRY_PORT);
    return (WorkerRemote) registry.lookup(WorkerTestDriver.SERVER_NAME);
  }

  public static void main(String[] args)
  {
    try {
      bindServer();
      log.debug("CoordinatorTestDriver bound");
    }
    catch (RemoteException e) {
      log.error("Error binding server", e);
    }
  }
}
