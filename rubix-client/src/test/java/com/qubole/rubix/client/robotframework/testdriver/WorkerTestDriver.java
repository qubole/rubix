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
import com.qubole.rubix.core.MockCachingFileSystem;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.shaded.TException;

import java.io.IOException;
import java.net.URI;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WorkerTestDriver implements WorkerRemote
{
  private static Log log = LogFactory.getLog(WorkerTestDriver.class);
  public static final String SERVER_NAME = "WorkerTestDriver";
  private static final int SERVER_PORT = 8564;

  private BookKeeperFactory factory = new BookKeeperFactory();
  private Configuration conf = new Configuration();

  public Task createTask(String remotePath,
                               long readStart,
                               int readLength,
                               long fileLength,
                               long lastModified,
                               int clusterType)
  {
    return new Task(new TestClientReadRequest(remotePath, readStart, readLength, fileLength, lastModified, clusterType));
  }

  @Override
  public boolean executeTask(Task task) throws RemoteException
  {
    TestClientReadRequest testClientReadRequest = task.getRequest();
    String remotePath = "file://" + testClientReadRequest.getRemotePath();
    try {
      FSDataInputStream fsstream = createFSInputStream(remotePath, (int) testClientReadRequest.getFileLength());
      byte[] buffer = new byte[testClientReadRequest.getReadLength()];
      int nread = fsstream.read(buffer, (int) testClientReadRequest.getReadStart(), testClientReadRequest.getReadLength());
      if (nread == testClientReadRequest.getReadLength()) {
        return true;
      }
    }
    catch (Exception ex) {
      log.info("Error while creating FSDataInputStream" + ex);
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
    try (RetryingBookkeeperClient client = factory.createBookKeeperClient(conf)) {
      Map<String, Double> metrics = client.getCacheMetrics();
      for (String metricName : metricsKeys) {
        testMetrics.put(metricName, metrics.get(metricName));
        log.info(String.format("Key: %s and Value: %d", metricName, metrics.get(metricName)));
      }
    }
    catch (TException | IOException e) {
      log.error("Error fetching BookKeeper metrics", e);
    }
    return testMetrics;
  }

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
