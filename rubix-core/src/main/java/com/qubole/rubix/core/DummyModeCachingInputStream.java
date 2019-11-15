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
package com.qubole.rubix.core;

import com.google.common.base.Throwables;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.shaded.transport.TTransportException;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class DummyModeCachingInputStream extends CachingInputStream
{
  private static final Log log = LogFactory.getLog(DummyModeCachingInputStream.class);

  public DummyModeCachingInputStream(FileSystem parentFs,
                                     Path backendPath,
                                     Configuration conf,
                                     CachingFileSystemStats statsMbean,
                                     BookKeeperFactory bookKeeperFactory,
                                     FileSystem remoteFileSystem,
                                     int bufferSize,
                                     FileSystem.Statistics statistics)
          throws IOException
  {
    super(parentFs, backendPath, conf, statsMbean, bookKeeperFactory, remoteFileSystem, bufferSize, statistics);
  }

  @Override
  public long getPos()
          throws IOException
  {
    return getParentDataInputStream().getPos();
  }

  @Override
  public void seek(long pos)
          throws IOException
  {
    checkState(pos >= 0, "Negative Position");
    getParentDataInputStream().seek(pos);
  }

  @Override
  public int read(final byte[] buffer, final int offset, final int length)
          throws IOException
  {
    final long initPos = getPos();
    final long initNextReadBlock = initPos / blockSize;
    final int read = readFullyDirect(buffer, offset, length);

    readService.execute(new Runnable()
    {
      @Override
      public void run()
      {
        try (RetryingBookkeeperClient client = bookKeeperFactory.createBookKeeperClient(conf)) {
          long endBlock = ((initPos + (length - 1)) / blockSize) + 1;
          final List<ReadRequestChain> readRequestChains = setupReadRequestChains(buffer, offset, endBlock, length,
              initPos, initNextReadBlock, client);
          updateCacheAndStats(readRequestChains);
        }
        catch (IOException e) {
          throw Throwables.propagate(e);
        }
        catch (TTransportException e) {
          log.warn("Could not create bookkeeper client", e);
          throw Throwables.propagate(e);
        }
      }
    });
    return read;
  }
}
