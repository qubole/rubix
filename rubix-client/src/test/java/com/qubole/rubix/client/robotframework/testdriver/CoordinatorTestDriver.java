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

import com.google.common.collect.Iterables;
import com.qubole.rubix.client.robotframework.TestClientReadRequest;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.qubole.rubix.common.metrics.BookKeeperMetrics.CacheMetric.CACHE_REQUEST_COUNT;
import static com.qubole.rubix.common.metrics.BookKeeperMetrics.CacheMetric.CACHE_SIZE_GAUGE;
import static com.qubole.rubix.common.metrics.BookKeeperMetrics.CacheMetric.NONLOCAL_REQUEST_COUNT;
import static com.qubole.rubix.common.metrics.BookKeeperMetrics.CacheMetric.REMOTE_REQUEST_COUNT;

public class CoordinatorTestDriver implements CoordinatorRemote
{
  private static Log log = LogFactory.getLog(CoordinatorTestDriver.class);
  public static final String SERVER_NAME = "CoordinatorTestDriver";
  private static final int MAX_THREADS = 10;
  private static final int SERVER_PORT = 8300;

  private List<String> clusterNodes;
  private Map<String, WorkerRemote> workerDriverMap = new HashMap<>();
  private BookKeeperFactory factory = new BookKeeperFactory();
  private Configuration conf = new Configuration();

  @Override
  public boolean executeJob(Job job) throws RemoteException
  {
    initializeWorkerReferences();

    Map<String, List<Task>> workerTaskMap = scheduleTasks(job);
    try {
      return executeWorkerTasks(workerTaskMap);
    }
    catch (InterruptedException | ExecutionException e) {
      log.error("Error executing tasks on worker nodes", e);
    }

    return false;
  }

  @Override
  public boolean verifyJob(Job job) throws RemoteException
  {
    printWorkerMetrics();
    return verifyMetrics(job);
  }

  private boolean verifyMetrics(Job job)
  {
    int totalRemoteRequestCount = 0;
    int totalCacheRequestCount = 0;
    int totalNonlocalRequestCount = 0;
    int totalCacheSize = 0;

    try {
      for (WorkerRemote worker : workerDriverMap.values()) {
        Map<String, Double> workerMetrics = worker.getTestMetrics(Arrays.asList(
            REMOTE_REQUEST_COUNT.getMetricName(),
            CACHE_REQUEST_COUNT.getMetricName(),
            NONLOCAL_REQUEST_COUNT.getMetricName(),
            CACHE_SIZE_GAUGE.getMetricName()));

        totalRemoteRequestCount += workerMetrics.get(REMOTE_REQUEST_COUNT.getMetricName());
        totalCacheRequestCount += workerMetrics.get(CACHE_REQUEST_COUNT.getMetricName());
        totalNonlocalRequestCount += workerMetrics.get(NONLOCAL_REQUEST_COUNT.getMetricName());
        // totalCacheSize += workerMetrics.get(CACHE_SIZE_GAUGE.getMetricName());
      }
      log.info(String.format(
          "@ Verification: %d RR, %d CR, %d NLR, %d cache size @",
          totalRemoteRequestCount,
          totalCacheRequestCount,
          totalNonlocalRequestCount,
          totalCacheSize));
    }
    catch (RemoteException e) {
      log.error("Error fetching test metrics from node xxx", e);
    }

    boolean expectedState = true;
    expectedState &= (totalRemoteRequestCount == job.getNumRemoteRequests());
    expectedState &= (totalCacheRequestCount == job.getNumCacheRequests());
    expectedState &= (totalNonlocalRequestCount == job.getNumNonLocalRequests());
    return expectedState;
  }

  private void printWorkerMetrics()
  {
    log.info(String.format("~ Printing metrics on workers (worker count: %d) ~", workerDriverMap.size()));
    try {
      for (WorkerRemote worker : workerDriverMap.values()) {
        worker.getCacheMetrics();
      }
      log.info("~ Finished printing ~");
    }
    catch (RemoteException e) {
      log.error("Error printing cache metrics on worker nodes", e);
    }
  }

  private void initializeWorkerReferences()
  {
    try (RetryingBookkeeperClient client = factory.createBookKeeperClient(conf)) {
      clusterNodes = client.getClusterNodes();
      log.warn(String.format("## Cluster nodes: %s ##", Arrays.toString(clusterNodes.toArray())));
      for (String node : clusterNodes) {
        WorkerRemote workerDriver = getWorkerTestDriverForNode(node);
        workerDriverMap.put(node, workerDriver);
      }
    }
    catch (TException | IOException | NotBoundException e) {
      log.error("Error initializing worker driver references", e);
    }
    log.info("Number of workers: " + workerDriverMap.size());
  }

  private Map<String, List<Task>> scheduleTasks(Job job)
  {
    Map<String, List<Task>> workerTaskMap = new HashMap<>();

    // Ratio:        2 1 2
    // Scaled ratio: 4 2 4
    // 0 1 2 3 4 5 6 7 8 9
    // |_____| |_| |_____|
    //    R     C     NL

    int remoteStart = 0;
    int remoteEnd = job.getNumRemoteRequests();
    List<Task> remoteRequests = job.getTasks().subList(remoteStart, remoteEnd);

    int cacheStart = remoteEnd;
    int cacheEnd = remoteEnd + job.getNumCacheRequests();
    List<Task> cachedRequests = job.getTasks().subList(cacheStart, cacheEnd);

    int nonlocalStart = cacheEnd;
    int nonlocalEnd = cacheEnd + job.getNumNonLocalRequests();
    List<Task> nonlocalRequests = job.getTasks().subList(nonlocalStart, nonlocalEnd);

    scheduleRemoteRequests(remoteRequests, workerTaskMap);
    scheduleCacheRequests(cachedRequests, workerTaskMap);
    scheduleNonlocalRequests(nonlocalRequests, workerTaskMap);

    log.info(String.format("Processed #s: %d R    %d C    %d NL", remoteRequests.size(), cachedRequests.size(), nonlocalRequests.size()));

    return workerTaskMap;
  }

  private void scheduleRemoteRequests(List<Task> remoteRequests, Map<String, List<Task>> workerTaskMap)
  {
    for (Task task : remoteRequests) {
      TestClientReadRequest request = task.getRequest();
      String expectedHostName = ConsistentHashUtil.getHashedNodeForKey(clusterNodes, request.getRemotePath() + "0" + request.getFileLength());
      addTaskForWorker(expectedHostName, task, workerTaskMap);
    }
  }

  private void scheduleCacheRequests(List<Task> cachedRequests, Map<String, List<Task>> workerTaskMap)
  {
    for (Task task : cachedRequests) {
      TestClientReadRequest request = task.getRequest();
      String expectedHostName = ConsistentHashUtil.getHashedNodeForKey(clusterNodes, request.getRemotePath() + "0" + request.getFileLength());
      preCacheFileOnWorker(expectedHostName, task);
      addTaskForWorker(expectedHostName, task, workerTaskMap);
    }
  }

  private void scheduleNonlocalRequests(List<Task> nonlocalRequests, Map<String, List<Task>> workerTaskMap)
  {
    for (Task task : nonlocalRequests) {
      String fileName = task.getRequest().getRemotePath();
      String nonlocalHostName = getOtherHostname(ConsistentHashUtil.getHashedNodeForKey(clusterNodes, fileName + "0" + task.getRequest().getFileLength()));
      addTaskForWorker(nonlocalHostName, task, workerTaskMap);
    }
  }

  private String getOtherHostname(String unwantedHostname)
  {
    Set<String> workerHostnames = new HashSet<>(clusterNodes);
    workerHostnames.remove(unwantedHostname);
    int randomHostnameIndex = new Random().nextInt(workerHostnames.size());
    return Iterables.get(workerHostnames, randomHostnameIndex);
  }

  private void preCacheFileOnWorker(String workerHostname, Task task)
  {
    try {
      log.warn(String.format("Pre-caching file %s on worker %s", task.getRequest().getRemotePath(), workerHostname));
      workerDriverMap.get(workerHostname).preCacheFile(task.getRequest());
    }
    catch (RemoteException e) {
      log.error(String.format("Error pre-caching file %s on worker %s", task.getRequest().getRemotePath(), workerHostname));
    }
  }

  private void addTaskForWorker(String workerHostname, Task task, Map<String, List<Task>> workerTaskMap)
  {
    List<Task> workerTasks = workerTaskMap.get(workerHostname);
    if (workerTasks == null) {
      workerTasks = new ArrayList<>();
      workerTaskMap.put(workerHostname, workerTasks);
    }
    workerTasks.add(task);
  }

  private boolean executeWorkerTasks(Map<String, List<Task>> taskMap) throws InterruptedException, ExecutionException
  {
    final ExecutorService service = Executors.newFixedThreadPool(MAX_THREADS);
    final List<Callable<Boolean>> callables = new ArrayList<>();

    for (Map.Entry<String, List<Task>> tasks : taskMap.entrySet()) {
      final String workerHostname = tasks.getKey();
      List<Task> workerTasks = tasks.getValue();

      log.info(String.format("=#= Worker %s : %d tasks =#=", workerHostname, workerTasks.size()));

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

  private static void bindServer() throws RemoteException
  {
    final CoordinatorRemote server = (CoordinatorRemote) UnicastRemoteObject.exportObject(new CoordinatorTestDriver(), SERVER_PORT);
    final Registry registry = LocateRegistry.getRegistry();
    registry.rebind(CoordinatorTestDriver.SERVER_NAME, server);
  }

  /**
   * Locates a {@link RequestServer} for executing requests on a particular container.
   *
   * @param host The hostname for the container to connect to.
   * @return The {@link RequestServer} used for executing requests.
   * @throws RemoteException   if the registry could not be located or communicated with.
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
