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
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.DataTransferClientHelper;
import com.qubole.rubix.spi.DataTransferHeader;
import com.qubole.rubix.spi.RetryingPooledBookkeeperClient;
import com.qubole.rubix.spi.thrift.CacheStatusRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;

import static com.google.common.base.Preconditions.checkState;

/**
 * Created by sakshia on 31/8/16.
 */
public class NonLocalReadRequestChain extends ReadRequestChain
{
  long fileSize;
  String filePath;
  long lastModified;
  String remoteNodeName;
  Configuration conf;
  int totalRead;
  int directRead;
  FileSystem remoteFileSystem;
  int clusterType;
  public boolean strictMode;
  FileSystem.Statistics statistics;

  DirectReadRequestChain directReadChain; // Used when Non Local Requests fail

  private static final Log log = LogFactory.getLog(NonLocalReadRequestChain.class);
  private BookKeeperFactory bookKeeperFactory = new BookKeeperFactory();

  public NonLocalReadRequestChain(String remoteLocation, long fileSize, long lastModified, Configuration conf,
                                  FileSystem remoteFileSystem, String remotePath, int clusterType,
                                  boolean strictMode, FileSystem.Statistics statistics)
  {
    this.remoteNodeName = remoteLocation;
    this.remoteFileSystem = remoteFileSystem;
    this.lastModified = lastModified;
    this.filePath = remotePath;
    this.fileSize = fileSize;
    this.conf = conf;
    this.clusterType = clusterType;
    this.strictMode = strictMode;
    this.statistics = statistics;
  }

  public ReadRequestChainStats getStats()
  {
    return new ReadRequestChainStats()
        .setNonLocalReads(requests)
        .setRequestedRead(directRead)
        .setNonLocalDataRead(totalRead);
  }

  @Override
  public Integer call()
      throws Exception
  {
    log.debug(String.format("Read Request threadName: %s, Non Local read Executor threadName: %s", threadName, Thread.currentThread().getName()));
    Thread.currentThread().setName(threadName);
    if (readRequests.size() == 0) {
      return 0;
    }
    checkState(isLocked, "Trying to execute Chain without locking");

    for (ReadRequest readRequest : readRequests) {
      if (cancelled) {
        propagateCancel(this.getClass().getName());
      }
      SocketChannel dataTransferClient;
      try {
        dataTransferClient = DataTransferClientHelper.createDataTransferClient(remoteNodeName, conf);
      }
      catch (Exception e) {
        log.warn("Could not create Data Transfer Client to node " + remoteNodeName, e);
        if (strictMode) {
          throw Throwables.propagate(e);
        }
        else {
          return directReadRequest(readRequests.indexOf(readRequest));
        }
      }
      try {
        int nread = 0;

        /*
        SocketChannels does not support timeouts when used directly, because timeout is used only by streams.
        We get this working by wrapping it in ReadableByteChannel.
        Ref - https://technfun.wordpress.com/2009/01/29/networking-in-java-non-blocking-nio-blocking-nio-and-io/
        */
        InputStream inStream = dataTransferClient.socket().getInputStream();
        ReadableByteChannel wrappedChannel = Channels.newChannel(inStream);

        ByteBuffer buf = DataTransferClientHelper.writeHeaders(conf, new DataTransferHeader(readRequest.getActualReadStart(),
            readRequest.getActualReadLength(), fileSize, lastModified, clusterType, filePath));

        dataTransferClient.write(buf);
        int bytesread = 0;
        ByteBuffer dst = ByteBuffer.wrap(readRequest.destBuffer, readRequest.getDestBufferOffset(), readRequest.destBuffer.length - readRequest.getDestBufferOffset());
        while (bytesread != readRequest.getActualReadLength()) {
          nread = wrappedChannel.read(dst);
          bytesread += nread;
          totalRead += nread;
          if (nread == -1) {
            totalRead -= bytesread;
            throw new Exception("Error reading from Local Transfer Server");
          }
          dst.position(bytesread + readRequest.getDestBufferOffset());
        }
      }
      catch (SocketTimeoutException e) {
        if (strictMode) {
          log.error(remoteNodeName + ": socket read timed out.", e);
          throw Throwables.propagate(e);
        }
        else {
          log.warn(remoteNodeName + ": socket read timed out. Using direct reads", e);
          return directReadRequest(readRequests.indexOf(readRequest));
        }
      }
      catch (Exception e) {
        log.warn("Error reading data from node : " + remoteNodeName, e);
        if (strictMode) {
          throw Throwables.propagate(e);
        }
        else {
          return directReadRequest(readRequests.indexOf(readRequest));
        }
      }
      finally {
        log.debug(String.format("Read %d bytes internally from node %s", totalRead, remoteNodeName));
        if (statistics != null) {
          statistics.incrementBytesRead(totalRead);
        }
        try {
          if (dataTransferClient != null) {
            dataTransferClient.close();
            dataTransferClient = null;
          }
        }
        catch (IOException e) {
          log.warn("Error closing Data Transfer client : " + remoteNodeName, e);
        }
      }
    }

    return totalRead;
  }

  @Override
  public void cancel()
  {
    super.cancel();
    if (directReadChain != null) {
      directReadChain.cancel();
    }
  }

  private int directReadRequest(int index)
      throws Exception
  {
    FSDataInputStream inputStream = remoteFileSystem.open(new Path(filePath));
    directReadChain = new DirectReadRequestChain(inputStream);
    for (ReadRequest readRequest : readRequests.subList(index, readRequests.size())) {
      directReadChain.addReadRequest(readRequest);
    }
    directReadChain.lock();
    directRead = directReadChain.call();
    inputStream.close();
    directReadChain = null;
    return (totalRead + directRead);
  }

  @Override
  public void updateCacheStatus(String remotePath, long fileSize, long lastModified, int blockSize, Configuration conf)
  {
    if (CacheConfig.isDummyModeEnabled(conf)) {
      try (RetryingPooledBookkeeperClient bookKeeperClient = bookKeeperFactory.createBookKeeperClient(remoteNodeName, conf)) {
        for (ReadRequest readRequest : readRequests) {
          long startBlock = toBlock(readRequest.getBackendReadStart());
          long endBlock = toBlock(readRequest.getBackendReadEnd() - 1) + 1;
          // getCacheStatus() call required to create mdfiles before blocks are set as cached
          CacheStatusRequest request = new CacheStatusRequest(remotePath, fileSize, lastModified, startBlock, endBlock, clusterType);
          bookKeeperClient.getCacheStatus(request);
          bookKeeperClient.setAllCached(remotePath, fileSize, lastModified, startBlock, endBlock);
        }
      }
      catch (Exception e) {
        if (strictMode) {
          throw Throwables.propagate(e);
        }
        log.error("Dummy Mode: Could not update Cache Status for Non-Local Read Request ", e);
      }
    }
  }

  private long toBlock(long pos)
  {
    long blockSize = CacheConfig.getBlockSize(conf);
    return pos / blockSize;
  }
}
