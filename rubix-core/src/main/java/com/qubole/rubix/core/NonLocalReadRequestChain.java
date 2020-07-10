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
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import static com.google.common.base.Preconditions.checkState;
import static com.qubole.rubix.spi.CacheUtil.DUMMY_MODE_GENERATION_NUMBER;
import static com.qubole.rubix.spi.CacheUtil.UNKONWN_GENERATION_NUMBER;
import static com.qubole.rubix.spi.DataTransferClientFactory.DataTransferClient;
import static com.qubole.rubix.spi.DataTransferClientFactory.getClient;

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
  long readFromNonLocalCache;
  long directRead;
  long directReadRequests;
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
    super(UNKONWN_GENERATION_NUMBER);
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
            .setNonLocalRRCDataRead(readFromNonLocalCache)
            .setNonLocalRRCRequests(directReadRequests == 0 ? requests : requests - directReadRequests)
            .setDirectRRCDataRead(directRead)
            .setDirectRRCRequests(directReadRequests);
  }

  @Override
  public Long call()
      throws Exception
  {
    log.debug(String.format("Read Request threadName: %s, Non Local read Executor threadName: %s", threadName, Thread.currentThread().getName()));
    Thread.currentThread().setName(threadName);
    if (readRequests.size() == 0) {
      return 0L;
    }
    checkState(isLocked, "Trying to execute Chain without locking");
    int dataReadInPreviousCycle = 0;
    for (ReadRequest readRequest : readRequests) {
      readFromNonLocalCache += dataReadInPreviousCycle;
      if (cancelled) {
        propagateCancel(this.getClass().getName());
      }
      try (DataTransferClient dataTransferClient = getClient(remoteNodeName, conf)) {
        int nread = 0;
        /*
        SocketChannels does not support timeouts when used directly, because timeout is used only by streams.
        We get this working by wrapping it in ReadableByteChannel.
        Ref - https://technfun.wordpress.com/2009/01/29/networking-in-java-non-blocking-nio-blocking-nio-and-io/
        */
        InputStream inStream = dataTransferClient.getSocketChannel().socket().getInputStream();
        ReadableByteChannel wrappedChannel = Channels.newChannel(inStream);
        ByteBuffer buf = DataTransferClientHelper.writeHeaders(conf, new DataTransferHeader(readRequest.getActualReadStart(),
            readRequest.getActualReadLengthIntUnsafe(), fileSize, lastModified, clusterType, filePath));

        try {
          dataTransferClient.getSocketChannel().write(buf);
        }
        catch (IOException e) {
          dataTransferClient.getSocketChannel().close();
          throw e;
        }
        int bytesread = 0;
        ByteBuffer dst = ByteBuffer.wrap(readRequest.destBuffer, readRequest.getDestBufferOffset(), readRequest.destBuffer.length - readRequest.getDestBufferOffset());
        while (bytesread != readRequest.getActualReadLengthIntUnsafe()) {
          try {
            nread = wrappedChannel.read(dst);
          }
          catch (IOException e) {
            log.warn("Error in reading..closing socket channel: " + dataTransferClient.getSocketChannel(), e);
            dataTransferClient.getSocketChannel().close();
            throw e;
          }
          if (nread == -1) {
            throw new Exception("Error reading from Local Transfer Server");
          }
          bytesread += nread;
          dst.position(bytesread + readRequest.getDestBufferOffset());
        }
        dataReadInPreviousCycle = bytesread;
      }
      catch (Exception e) {
        if (strictMode) {
          log.warn("Error reading data from node : " + remoteNodeName, e);
          throw Throwables.propagate(e);
        }
        else {
          log.warn("Error in reading from node: " + remoteNodeName + " Using direct reads", e);
          return directReadRequest(readRequests.indexOf(readRequest));
        }
      }
      finally {
        if (statistics != null) {
          statistics.incrementBytesRead(readFromNonLocalCache);
        }
      }
    }
    readFromNonLocalCache += dataReadInPreviousCycle;
    log.debug(String.format("Read %d bytes internally from node %s", readFromNonLocalCache, remoteNodeName));
    return readFromNonLocalCache;
  }

  @Override
  public void cancel()
  {
    super.cancel();
    if (directReadChain != null) {
      directReadChain.cancel();
    }
  }

  private long directReadRequest(int index)
      throws Exception
  {
    try (FSDataInputStream inputStream = remoteFileSystem.open(new Path(filePath))) {
      directReadChain = new DirectReadRequestChain(inputStream);
      for (ReadRequest readRequest : readRequests.subList(index, readRequests.size())) {
        directReadChain.addReadRequest(readRequest);
        directReadRequests++;
      }
      directReadChain.lock();
      directRead = directReadChain.call();
      directReadChain = null;
    }
    return (readFromNonLocalCache + directRead);
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
          CacheStatusRequest request = new CacheStatusRequest(remotePath, fileSize, lastModified, startBlock, endBlock).setClusterType(clusterType);
          bookKeeperClient.getCacheStatus(request);
          bookKeeperClient.setAllCached(remotePath, fileSize, lastModified, startBlock, endBlock, DUMMY_MODE_GENERATION_NUMBER);
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
