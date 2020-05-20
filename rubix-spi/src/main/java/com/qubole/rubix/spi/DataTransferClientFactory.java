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

import com.google.common.annotations.VisibleForTesting;
import com.qubole.rubix.spi.fop.ObjectPool;
import com.qubole.rubix.spi.fop.Poolable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.qubole.rubix.spi.fop.SocketChannelObjectFactory.createSocketChannelObjectPool;

public class DataTransferClientFactory
{
  private static final AtomicBoolean initFlag = new AtomicBoolean();
  private static ObjectPool pool;
  private static final Log log = LogFactory.getLog(DataTransferClientHelper.class.getName());

  private DataTransferClientFactory()
  {
  }

  @VisibleForTesting
  public static void resetConnectionPool()
  {
    initFlag.set(false);
    pool = null;
  }

  public static DataTransferClient getClient(String host, Configuration conf)
  {
    if (!initFlag.get()) {
      synchronized (initFlag) {
        if (!initFlag.get()) {
          pool = createSocketChannelObjectPool(conf, host, CacheConfig.getDataTransferServerPort(conf));
          initFlag.set(true);
        }
      }
    }
    Poolable<SocketChannel> socketChannelPoolable = pool.borrowObject(host, conf);
    return new DataTransferClient(socketChannelPoolable);
  }

  public static class DataTransferClient
    implements Closeable
  {
    private Poolable<SocketChannel> socketChannelPoolable;
    private static final Log log = LogFactory.getLog(DataTransferClientHelper.class.getName());

    public DataTransferClient(Poolable<SocketChannel> socketChannelPoolable)
    {
      this.socketChannelPoolable = socketChannelPoolable;
    }

    public SocketChannel getSocketChannel()
    {
      return socketChannelPoolable.getObject();
    }

    @Override
    public void close()
    {
      log.debug("Closing socket channel: " + socketChannelPoolable.getObject());
      socketChannelPoolable.getPool().returnObject(socketChannelPoolable);
    }
  }
}
