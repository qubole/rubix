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
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;

public class SocketObjectFactory
    implements ObjectFactory<TSocket>
{
  private static final Log log = LogFactory.getLog(SocketObjectFactory.class.getName());

  private final int port;

  public SocketObjectFactory(int port)
  {
    this.port = port;
  }

  @Override
  public TSocket create(String host, int socketTimeout, int connectTimeout)
  {
    log.debug("Opening connection to host: " + host);
    TSocket socket = null;
    try {
      socket = new TSocket(host, port, socketTimeout, connectTimeout);
      socket.open();
    }
    catch (TTransportException e) {
      socket = null;
      log.warn("Unable to open connection to host " + host, e);
    }
    return socket;
  }

  @Override
  public void destroy(TSocket o)
  {
    // clean up and release resources
    o.close();
  }

  @Override
  public boolean validate(TSocket o)
  {
    boolean isClosed = (o != null && !o.isOpen());

    // Saw that transport.close did not close socket, explicitly closing socket
    if (isClosed && o != null) {
      try {
        o.getSocket().close();
      }
      catch (IOException e) {
        // Let os time it out
      }
    }

    return !isClosed;
  }

  public static ObjectPool<TSocket> createSocketObjectPool(Configuration conf, String host, int port)
  {
    PoolConfig poolConfig = new PoolConfig();
    poolConfig.setMaxSize(CacheConfig.getTranportPoolMaxSize(conf));
    poolConfig.setMinSize(CacheConfig.getTransportPoolMinSize(conf));
    poolConfig.setDelta(CacheConfig.getTransportPoolDeltaSize(conf));
    poolConfig.setMaxWaitMilliseconds(CacheConfig.getTransportPoolMaxWait(conf));

    ObjectFactory<TSocket> factory = new SocketObjectFactory(port);
    ObjectPool<TSocket> pool = new ObjectPool(poolConfig, factory);
    pool.registerHost(host, CacheConfig.getServerSocketTimeout(conf), CacheConfig.getServerConnectTimeout(conf));
    return pool;
  }
}
