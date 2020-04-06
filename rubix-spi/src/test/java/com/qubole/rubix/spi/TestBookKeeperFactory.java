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
package com.qubole.rubix.spi;

import com.qubole.rubix.spi.thrift.BookKeeperService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

public class TestBookKeeperFactory
{
  private static final Log log = LogFactory.getLog(TestBookKeeperFactory.class);

  private static final int NO_DELAY = 0;
  private static final int WAIT_FOR_BKS_START = 200;

  private final Configuration conf = new Configuration();
  private MockBookKeeperServer server;
  private BookKeeperFactory bookKeeperFactory;

  @BeforeMethod
  public void setUp()
  {
    bookKeeperFactory = new BookKeeperFactory();
  }

  @AfterMethod
  public void tearDown()
  {
    if (server != null) {
      stopMockServer();
    }
    conf.clear();
  }

  @Test
  public void testCreateBookKeeperClient_ableToConnect() throws TException, InterruptedException
  {
    final int connectTimeout = 500;
    final int socketTimeout = 500;

    server = startMockServer(true, NO_DELAY, NO_DELAY);

    final RetryingPooledBookkeeperClient client = createTestBookKeeperClient(socketTimeout, connectTimeout);
    assertTrue(client.isBookKeeperAlive());

    stopMockServer();
  }

  @Test
  public void testCreateBookKeeperClient_ableToConnect_startDelay() throws TException, InterruptedException
  {
    final int startDelay = 500;
    final int connectTimeout = 1000;
    final int socketTimeout = 500;

    server = startMockServer(true, startDelay, NO_DELAY);

    final RetryingPooledBookkeeperClient client = createTestBookKeeperClient(socketTimeout, connectTimeout);
    assertTrue(client.isBookKeeperAlive());

    stopMockServer();
  }

  @Test
  public void testCreateBookKeeperClient_ableToConnect_aliveCallDelay() throws TException, InterruptedException
  {
    final int connectTimeout = 500;
    final int aliveCallDelay = 500;
    final int socketTimeout = 1000;

    server = startMockServer(true, NO_DELAY, aliveCallDelay);

    final RetryingPooledBookkeeperClient client = createTestBookKeeperClient(socketTimeout, connectTimeout);
    assertTrue(client.isBookKeeperAlive());

    stopMockServer();
  }

  @Test
  public void testCreateBookKeeperClient_ableToConnect_startDelayAndAliveCallDelay() throws TException, InterruptedException
  {
    final int startDelay = 500;
    final int connectTimeout = 1000;
    final int aliveCallDelay = 500;
    final int socketTimeout = 1000;

    server = startMockServer(true, startDelay, aliveCallDelay);

    final RetryingPooledBookkeeperClient client = createTestBookKeeperClient(socketTimeout, connectTimeout);
    assertTrue(client.isBookKeeperAlive());

    stopMockServer();
  }

  @Test(expectedExceptions = TTransportException.class)
  public void testCreateBookKeeperClient_unableToConnect_connectTimeout() throws TException, InterruptedException
  {
    final int startDelay = 1000;
    final int connectTimeout = 500;
    final int socketTimeout = 500;

    server = startMockServer(false, startDelay, NO_DELAY);

    createTestBookKeeperClient(socketTimeout, connectTimeout); // should throw expected exception due to connect timeout
  }

  @Test(expectedExceptions = TTransportException.class)
  public void testCreateBookKeeperClient_unableToConnect_socketTimeout() throws TException, InterruptedException
  {
    final int connectTimeout = 500;
    final int aliveCallDelay = 1000;
    final int socketTimeout = 500;

    server = startMockServer(true, NO_DELAY, aliveCallDelay);

    final RetryingPooledBookkeeperClient client = createTestBookKeeperClient(socketTimeout, connectTimeout);
    client.isBookKeeperAlive(); // should throw expected exception due to socket timeout
  }

  @Test(expectedExceptions = TTransportException.class)
  public void testCreateBookKeeperClient_startDelay_unableToConnect_socketTimeout() throws TException, InterruptedException
  {
    final int startDelay = 500;
    final int connectTimeout = 1000;
    final int aliveCallDelay = 1000;
    final int socketTimeout = 500;

    server = startMockServer(true, startDelay, aliveCallDelay);

    final RetryingPooledBookkeeperClient client = createTestBookKeeperClient(socketTimeout, connectTimeout);
    client.isBookKeeperAlive(); // should throw expected exception due to socket timeout
  }

  private MockBookKeeperServer startMockServer(boolean waitForStart, int startDelay, int aliveCallDelay) throws InterruptedException
  {
    MockBookKeeperServer server = new MockBookKeeperServer(startDelay, aliveCallDelay);
    server.start();

    if (waitForStart) {
      while (!server.isServerUp()) {
        Thread.sleep(WAIT_FOR_BKS_START);
        log.info("Waiting for MockBookKeeperServer to come up");
      }
    }

    return server;
  }

  private void stopMockServer()
  {
    server.stopServer();
    server = null;
  }

  private RetryingPooledBookkeeperClient createTestBookKeeperClient(int socketTimeout, int connectTimeout) throws TTransportException
  {
    CacheConfig.setServerSocketTimeout(conf, socketTimeout);
    CacheConfig.setServerConnectTimeout(conf, connectTimeout);

    return bookKeeperFactory.createBookKeeperClient(conf);
  }

  private class MockBookKeeperServer extends Thread
  {
    private final int startDelay;
    private final int aliveCallDelay;
    private TServer server;

    public MockBookKeeperServer(int startDelay, int aliveCallDelay)
    {
      this.startDelay = startDelay;
      this.aliveCallDelay = aliveCallDelay;
    }

    @Override
    public void run()
    {
      try {
        Thread.sleep(startDelay);
        startServer(conf);
      }
      catch (InterruptedException | TException e) {
        log.error("Error when starting MockBookKeeperServer", e);
      }
    }

    void startServer(Configuration conf) throws TException
    {
      BookKeeperService.Iface bookKeeper = mock(BookKeeperService.Iface.class);
      when(bookKeeper.isBookKeeperAlive()).then(new GetBookKeeperAliveStatus());

      try {
        TServerTransport serverTransport = new TServerSocket(CacheConfig.getBookKeeperServerPort(conf));
        BookKeeperService.Processor processor = new BookKeeperService.Processor<>(bookKeeper);
        server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
        server.serve();
      }
      catch (TException e) {
        log.error("Error starting MockBookKeeperServer", e);
      }
    }

    void stopServer()
    {
      if (server != null) {
        server.stop();
      }
    }

    boolean isServerUp()
    {
      if (server != null) {
        return server.isServing();
      }
      return false;
    }

    /**
     * Answer for responding to isBookKeeperAlive client call with a specified delay.
     */
    public class GetBookKeeperAliveStatus implements Answer<Boolean>
    {
      @Override
      public Boolean answer(InvocationOnMock invocation)
      {
        try {
          Thread.sleep(aliveCallDelay);
        }
        catch (InterruptedException e) {
          log.error("Thread interrupted while delaying alive call", e);
        }
        return true;
      }
    }
  }
}
