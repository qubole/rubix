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
import com.qubole.rubix.spi.ClusterType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

public class DummyModeCachingInputStream extends CachingInputStream
{
  private static final Log log = LogFactory.getLog(DummyModeCachingInputStream.class);

  public DummyModeCachingInputStream(FileSystem parentFs,
                                     Path backendPath,
                                     Configuration conf,
                                     CachingFileSystemStats statsMbean,
                                     ClusterType clusterType,
                                     BookKeeperFactory bookKeeperFactory,
                                     FileSystem remoteFileSystem,
                                     int bufferSize,
                                     FileSystem.Statistics statistics)
          throws IOException
  {
    super(backendPath, conf, statsMbean, clusterType, bookKeeperFactory, remoteFileSystem, bufferSize, statistics);
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
    getParentDataInputStream().seek(pos);
  }

  @Override
  public int read(byte[] buffer, int offset, int length)
          throws IOException
  {
    long initPos = getPos();
    int read = readFullyDirect(buffer, offset, length);

    dummyRead(initPos, buffer, offset, length);
    return read;
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length)
          throws IOException
  {
    long initPos = getPos();
    getParentDataInputStream().readFully(position, buffer, offset, length);

    dummyRead(initPos, buffer, offset, length);
    return length;
  }

  private void dummyRead(final long initPos, final byte[] buffer, final int offset, final int length)
  {
    final long initNextReadBlock = initPos / blockSize;
    readService.execute(new Runnable()
    {
      @Override
      public void run()
      {
        try {
          long endBlock = ((initPos + (length - 1)) / blockSize) + 1;
          final List<ReadRequestChain> readRequestChains = setupReadRequestChains(buffer,
              offset,
              endBlock,
              length,
              initPos,
              initNextReadBlock);
          updateCacheAndStats(readRequestChains);
        }
        catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
    });
  }
}
