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

import com.qubole.rubix.client.robotframework.TestClientReadRequest;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import com.qubole.rubix.spi.thrift.BlockLocation;
import com.qubole.rubix.spi.thrift.CacheStatusRequest;
import com.qubole.rubix.spi.thrift.Location;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.shaded.TException;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.qubole.rubix.common.metrics.BookKeeperMetrics.CacheMetric.CACHE_REQUEST_COUNT;
import static com.qubole.rubix.common.metrics.BookKeeperMetrics.CacheMetric.CACHE_SIZE_GAUGE;
import static com.qubole.rubix.common.metrics.BookKeeperMetrics.CacheMetric.NONLOCAL_REQUEST_COUNT;
import static com.qubole.rubix.common.metrics.BookKeeperMetrics.CacheMetric.REMOTE_REQUEST_COUNT;

public class WorkerTestDriver implements WorkerRemote
{
  private static Log log = LogFactory.getLog(WorkerTestDriver.class);
  public static final String SERVER_NAME = "WorkerTestDriver";
  private static final int SERVER_PORT = 8564;

  private BookKeeperFactory factory = new BookKeeperFactory();
  private Configuration conf = new Configuration();
  private AtomicInteger remoteRequests = new AtomicInteger();
  private AtomicInteger cacheRequests = new AtomicInteger();
  private AtomicInteger nonlocalRequests = new AtomicInteger();

  @Override
  public boolean executeTask(Task task) throws RemoteException
  {
    try (RetryingBookkeeperClient client = factory.createBookKeeperClient(conf)) {
      TestClientReadRequest request = task.getRequest();
      List<BlockLocation> status = client.getCacheStatus(new CacheStatusRequest(
          request.getRemotePath(),
          request.getFileLength(),
          request.getLastModified(),
          request.getReadStart(),
          request.getReadLength(),
          request.getClusterType()));

      Location fileStatus = status.get(0).getLocation();
      switch (fileStatus) {
        case LOCAL:
          remoteRequests.getAndIncrement();
          break;
        case CACHED:
          cacheRequests.getAndIncrement();
          break;
        case NON_LOCAL:
          nonlocalRequests.getAndIncrement();
          break;
      }
      return true;
    }
    catch (TException | IOException e) {
      log.error(String.format("Error executing task %s", task.toString()), e);
    }
    return false;
  }

  @Override
  public boolean preCacheFile(TestClientReadRequest request) throws RemoteException
  {
    try (RetryingBookkeeperClient client = factory.createBookKeeperClient(conf)) {
      return client.readData(
          "file:" + request.getRemotePath(),
          request.getReadStart(),
          request.getReadLength(),
          request.getFileLength(),
          request.getLastModified(),
          request.getClusterType());
    }
    catch (TException | IOException e) {
      log.error(String.format("Error pre-caching file %s", request.getRemotePath()), e);
    }
    return false;
  }

  @Override
  public Map<String, Double> getTestMetrics(List<String> metricsKeys) throws RemoteException
  {
    Map<String, Double> testMetrics = new HashMap<>();
    testMetrics.put(REMOTE_REQUEST_COUNT.getMetricName(), (double) remoteRequests.get());
    testMetrics.put(CACHE_REQUEST_COUNT.getMetricName(), (double) cacheRequests.get());
    testMetrics.put(NONLOCAL_REQUEST_COUNT.getMetricName(), (double) nonlocalRequests.get());

    try (RetryingBookkeeperClient client = factory.createBookKeeperClient(conf)) {
      Map<String, Double> metrics = client.getCacheMetrics();

      testMetrics.put(CACHE_SIZE_GAUGE.getMetricName(), metrics.get(CACHE_SIZE_GAUGE.getMetricName()));
    }
    catch (TException | IOException e) {
      log.error("Error fetching BookKeeper metrics", e);
    }

    return testMetrics;
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
