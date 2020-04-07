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
package com.qubole.rubix.spi.client;

import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.fop.ObjectPool;
import com.qubole.rubix.spi.fop.SocketObjectFactory;
import com.qubole.rubix.spi.thrift.Request;
import com.qubole.rubix.spi.thrift.TestingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import static com.qubole.rubix.spi.CacheConfig.getServerMaxThreads;
import static org.testng.Assert.assertTrue;

public class TestPoolingClient
{
  private static final int PORT = 8656;
  private static TServer server;
  private static Configuration conf;

  @BeforeClass
  public static void setup()
  {
    conf = new Configuration();
    // min=max=1 => we get same transport always
    CacheConfig.setTranportPoolMinSize(conf, 1);
    CacheConfig.setTranportPoolMaxSize(conf, 1);
    CacheConfig.setServerSocketTimeout(conf, 500);
    CacheConfig.setServerConnectTimeout(conf, 1000);

    startServerAsync(conf);
  }

  @Test
  public void testRetryHappens()
      throws TException, InterruptedException
  {
    ObjectPool pool = SocketObjectFactory.createSocketObjectPool(conf, "localhost", PORT);
    RetryingPooledThriftTestClient client = new RetryingPooledThriftTestClient(
        20,
        conf,
        "localhost",
        pool.borrowObject("localhost", conf));

    //  issue a request which is sure to hit SocketTimeout exception
    final Request request = new Request("_Message_", 2);
    final AtomicReference<String> message = new AtomicReference<>();

    // Start a Thread that removes sleep after some time
    // If retries are in place, the response will be received after request has been modified
    new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        try {
          Thread.sleep(2000);
        }
        catch (InterruptedException e) {
          e.printStackTrace();
        }
        assertTrue(message.get() == null, "Request should not have finished");
        request.setSleepSecs(0);
      }
    }).start();

    message.set(client.echo(request));

    // given enough retries, we should have the response
    assertTrue(message.get().equals("_Message_"), "Wrong response read: " + message.get());
  }

  @Test
  public void testNoConnectionReuseOnException()
      throws TException, InterruptedException
  {
    ObjectPool pool = SocketObjectFactory.createSocketObjectPool(conf, "localhost", PORT);
    RetryingPooledThriftTestClient client = new RetryingPooledThriftTestClient(
        1,
        conf,
        "localhost",
        pool.borrowObject("localhost", conf));

    // First issue a request which is sure to hit SocketTimeout exception
    try {
      client.echo(new Request("FirstMessage", 10));
    }
    catch (TException e) {
      // expected
    }

    // Sleep more than 2 seconds to get the response back from server
    Thread.sleep(3);
    String message = client.echo(new Request("SecondMessage", 0));

    assertTrue(message.equals("SecondMessage"), "Wrong response read: " + message);
  }

  private static void startServerAsync(final Configuration conf)
  {
    new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        try {
          startServer(conf);
        }
        catch (TTransportException e) {
          e.printStackTrace();
        }
      }
    }).start();

    while (!(server != null && server.isServing())) {
      try {
        Thread.sleep(100);
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private static void startServer(final Configuration conf)
      throws TTransportException
  {
    if (server != null) {
      return;
    }

    Processor processor = new Processor();
    TServerTransport serverTransport = new TServerSocket(
        new TServerSocket.ServerSocketTransportArgs().bindAddr(new InetSocketAddress(PORT)).backlog(Integer.MAX_VALUE));
    server = new TThreadPoolServer(new TThreadPoolServer
        .Args(serverTransport)
        .processor(new TestingService.Processor(processor))
        .maxWorkerThreads(getServerMaxThreads(conf)));

    server.serve();
  }

  @AfterClass
  public static void cleanup()
  {
    if (server != null) {
      server.stop();
      server = null;
    }
  }
}
