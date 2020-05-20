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
package com.qubole.rubix.spi.fop;

import com.qubole.rubix.spi.CacheConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

public class SocketChannelObjectFactory
    implements ObjectFactory<SocketChannel>
{
  private static final Log log = LogFactory.getLog(SocketChannelObjectFactory.class.getName());
  private static final String LDS_POOL = "lds-pool";

  private final int port;

  public SocketChannelObjectFactory(int port)
  {
    this.port = port;
  }

  @Override
  public SocketChannel create(String host, int socketTimeout, int connectTimeout)
  {
    SocketAddress sad = new InetSocketAddress(host, this.port);
    SocketChannel socket = null;
    try {
      socket = SocketChannel.open();
      socket.socket().setSoTimeout(socketTimeout);
      socket.socket().connect(sad, connectTimeout);
      log.debug(LDS_POOL + " : Socket channel connected to host: " + host +
              " with local socket address: " + socket.socket().getLocalSocketAddress());
    }
    catch (IOException e) {
      e.printStackTrace();
      log.warn(LDS_POOL + " : Unable to open connection to host " + host, e);
    }
    return socket;
  }

  @Override
  public void destroy(SocketChannel o)
  {
    // clean up and release resources
    log.debug(LDS_POOL + " : Destroy socket channel: " + o);
    try {
      o.close();
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public boolean validate(SocketChannel o)
  {
    boolean isClosed = (o != null && !o.isOpen());

    // Saw that transport.close did not close socket, explicitly closing socket
    if (isClosed && o != null) {
      try {
        o.socket().close();
      }
      catch (IOException e) {
        // Let os time it out
      }
    }
    log.debug(LDS_POOL + " : Validate socket channel: " + o + " isvalid: " + !isClosed);
    return !isClosed;
  }

  public static ObjectPool<SocketChannel> createSocketChannelObjectPool(Configuration conf, String host, int port)
  {
    log.debug(LDS_POOL + " : Creating socket channel object pool");
    PoolConfig poolConfig = new PoolConfig();
    poolConfig.setMaxSize(CacheConfig.getTranportPoolMaxSize(conf));
    poolConfig.setMinSize(CacheConfig.getTransportPoolMinSize(conf));
    poolConfig.setDelta(1);
    poolConfig.setMaxWaitMilliseconds(CacheConfig.getTransportPoolMaxWait(conf));
    poolConfig.setScavengeIntervalMilliseconds(60000);

    ObjectFactory<SocketChannel> factory = new SocketChannelObjectFactory(port);
    ObjectPool<SocketChannel> pool = new ObjectPool(poolConfig, factory, LDS_POOL);
    pool.registerHost(host, CacheConfig.getClientReadTimeout(conf), CacheConfig.getClientReadTimeout(conf));
    return pool;
  }
}
