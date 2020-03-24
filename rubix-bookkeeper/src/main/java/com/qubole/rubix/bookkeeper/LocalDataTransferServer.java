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
package com.qubole.rubix.bookkeeper;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.CachedThreadStatesGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.google.common.annotations.VisibleForTesting;
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import com.qubole.rubix.spi.DataTransferClientHelper;
import com.qubole.rubix.spi.DataTransferHeader;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import com.qubole.rubix.spi.thrift.BlockLocation;
import com.qubole.rubix.spi.thrift.CacheStatusRequest;
import com.qubole.rubix.spi.thrift.Location;
import com.qubole.rubix.spi.thrift.ReadDataRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.thrift.shaded.TException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by sakshia on 26/10/16.
 */

public class LocalDataTransferServer extends Configured implements Tool
{
  private static Log log = LogFactory.getLog(LocalDataTransferServer.class.getName());
  private static Configuration conf;
  private static LocalServer localServer;
  private static MetricRegistry metrics;
  private static BookKeeperMetrics bookKeeperMetrics;

  private LocalDataTransferServer()
  {
  }

  public static void main(String[] args) throws Exception
  {
    ToolRunner.run(new Configuration(), new LocalDataTransferServer(), args);
  }

  @Override
  public int run(String[] args) throws Exception
  {
    conf = this.getConf();
    startServer(conf, new MetricRegistry());
    return 0;
  }

  public static void startServer(Configuration conf, MetricRegistry metricRegistry)
  {
    startServer(conf, metricRegistry, null);
  }

  // In embedded mode, this is called directly with local bookKeeper object
  public static void startServer(Configuration conf, MetricRegistry metricRegistry, BookKeeper bookKeeper)
  {
    conf = new Configuration(conf);
    CacheConfig.setCacheDataEnabled(conf, false);
    CacheConfig.disableFSCaches(conf);
    metrics = metricRegistry;
    registerMetrics(conf);

    localServer = new LocalServer(conf, bookKeeper);
    new Thread(localServer).start();
  }

  /**
   * Register desired metrics.
   *
   * @param conf The current Hadoop configuration.
   */
  private static void registerMetrics(Configuration conf)
  {
    bookKeeperMetrics = new BookKeeperMetrics(conf, metrics);

    metrics.register(BookKeeperMetrics.LDTSJvmMetric.LDTS_JVM_GC_PREFIX.getMetricName(), new GarbageCollectorMetricSet());
    metrics.register(BookKeeperMetrics.LDTSJvmMetric.LDTS_JVM_THREADS_PREFIX.getMetricName(), new CachedThreadStatesGaugeSet(CacheConfig.getMetricsReportingInterval(conf), TimeUnit.MILLISECONDS));
    metrics.register(BookKeeperMetrics.LDTSJvmMetric.LDTS_JVM_MEMORY_PREFIX.getMetricName(), new MemoryUsageGaugeSet());
  }

  public static void stopServer()
  {
    removeMetrics();
    if (localServer != null) {
      try {
        bookKeeperMetrics.close();
      }
      catch (IOException e) {
        log.error("Metrics reporters could not be closed", e);
      }
      localServer.stop();
    }
  }

  protected static void removeMetrics()
  {
    metrics.removeMatching(bookKeeperMetrics.getMetricsFilter());
  }

  @VisibleForTesting
  public static boolean isServerUp()
  {
    if (localServer != null) {
      return localServer.isAlive();
    }

    return false;
  }

  public static class LocalServer
      implements Runnable
  {
    static ServerSocketChannel listener;
    Configuration conf;
    BookKeeperFactory bookKeeperFactory;

    public LocalServer(Configuration conf, BookKeeper bookKeeper)
    {
      this(conf, new BookKeeperFactory(bookKeeper));
    }

    public LocalServer(Configuration conf, BookKeeperFactory bookKeeperFactory)
    {
      this.conf = conf;
      this.bookKeeperFactory = bookKeeperFactory;
    }

    @Override
    public void run()
    {
      int port = CacheConfig.getDataTransferServerPort(conf);
      ExecutorService threadPool = Executors.newCachedThreadPool();
      try {
        listener = ServerSocketChannel.open();
        listener.bind(new InetSocketAddress(port), Integer.MAX_VALUE);
        log.info("Started LocalDataTransferServer on port " + port);
        while (true) {
          SocketChannel clientSocket = listener.accept();
          ClientServiceThread cliThread = new ClientServiceThread(clientSocket, conf, bookKeeperFactory);
          threadPool.execute(cliThread);
        }
      }
      catch (AsynchronousCloseException e) {
        log.warn("Stopping Local Transfer server", e);
      }
      catch (IOException e) {
        log.error("Error starting Local Transfer server", e);
      }
    }

    public boolean isAlive()
    {
      return listener.isOpen();
    }

    public void stop()
    {
      try {
        listener.close();
      }
      catch (IOException e) {
        log.error("Error stopping Local Transfer server", e);
      }
    }
  }

  static class ClientServiceThread
      extends Thread
  {
    SocketChannel localDataTransferClient;
    Configuration conf;
    BookKeeperFactory bookKeeperFactory;

    ClientServiceThread(SocketChannel s, Configuration conf, BookKeeperFactory bookKeeperFactory)
    {
      localDataTransferClient = s;
      this.conf = conf;
      this.bookKeeperFactory = bookKeeperFactory;
    }

    public void run()
    {
      try {
        log.debug("Connected to node - " + localDataTransferClient.getRemoteAddress());
        ByteBuffer dataInfo = ByteBuffer.allocate(CacheConfig.getMaxHeaderSize(conf));

        int read = localDataTransferClient.read(dataInfo);
        if (read == -1) {
          throw new Exception("Could not read data from Non-local node");
        }
        dataInfo.flip();

        DataTransferHeader header = DataTransferClientHelper.readHeaders(dataInfo);
        long offset = header.getOffset();
        int readLength = header.getReadLength();
        String remotePath = header.getFilePath();
        log.debug(String.format("Trying to read from %s at offset %d and length %d for client %s", remotePath, offset, readLength, localDataTransferClient.getRemoteAddress()));
        try (RetryingBookkeeperClient bookKeeperClient = bookKeeperFactory.createBookKeeperClient(conf)) {
          if (!CacheConfig.isParallelWarmupEnabled(conf)) {
            ReadDataRequest readDataRequest = new ReadDataRequest(remotePath, offset, readLength, header.getFileSize(), header.getLastModified());
            if (!bookKeeperClient.readData(readDataRequest)) {
              throw new Exception("Could not cache data required by non-local node");
            }
          }
          else {
            // Just make sure the requested blocks are present in the cache. If any the blocks is
            // not presnt in the cache, throw an exception so that the caller NonLocalReadRequestChain
            // can read the data from the object store
            long blockSize = CacheConfig.getBlockSize(conf);
            long startBlock = offset / blockSize;
            long endBlock = ((offset + (readLength - 1)) / blockSize) + 1;

            CacheStatusRequest request = new CacheStatusRequest(remotePath, header.getFileSize(), header.getLastModified(),
                startBlock, endBlock);
            List<BlockLocation> blockLocations = bookKeeperClient.getCacheStatus(request);

            long blockNum = startBlock;
            for (BlockLocation location : blockLocations) {
              if (location.getLocation() != Location.CACHED) {
                log.error(String.format("The requested data for block %d of file %s in not in cache. " +
                    " The data will be read from object store", blockNum, remotePath));
                throw new Exception("The requested data in not in cache. The data will be read from object store");
              }
              blockNum++;
            }
          }

          int nread = readDataFromCachedFile(bookKeeperClient, remotePath, offset, readLength);
          log.debug(String.format("Done reading %d from %s at offset %d and length %d for client %s", nread, remotePath, offset, readLength, localDataTransferClient.getRemoteAddress()));
        }
      }
      catch (Exception e) {
        try {
          log.warn("Error in Local Data Transfer Server for client: " + localDataTransferClient.getRemoteAddress(), e);
        }
        catch (IOException e1) {
          log.warn("Error in Local Data Transfer Server for client: ", e);
        }
        return;
      }
      finally {
        try {
          localDataTransferClient.close();
        }
        catch (IOException e) {
          log.warn("Error in Local Data Transfer Server: ", e);
        }
      }
    }

    private int readDataFromCachedFile(RetryingBookkeeperClient bookKeeperClient, String remotePath, long offset, int readLength) throws IOException, TException
    {
      FileChannel fc = null;
      int nread = 0;
      String filename = CacheUtil.getLocalPath(remotePath, conf);

      try {
        fc = new FileInputStream(filename).getChannel();
        int maxCount = CacheConfig.getLocalTransferBufferSize(conf);
        int lengthRemaining = readLength;
        long position = offset;

        // This situation should not arise as ActualReadLength cannot be greater than the file size.
        // This seems to case of corrupted file. We should invalidate the file in this case.
        if (fc.size() < readLength) {
          log.error(String.format("File size is smaller than requested read. Invalidating corrupted cached file %s", remotePath));
          bookKeeperClient.invalidateFileMetadata(remotePath);
          throw new IOException("File size is smaller than requested read");
        }

        while (nread < readLength) {
          if (maxCount > lengthRemaining) {
            maxCount = lengthRemaining;
          }
          nread += fc.transferTo(position + nread, maxCount, localDataTransferClient);
          lengthRemaining = readLength - nread;
        }
      }
      catch (FileNotFoundException ex) {
        log.error(String.format("Could not create file channel for %s. Invalidating missing remote file %s", filename, remotePath));
        bookKeeperClient.invalidateFileMetadata(remotePath);
        throw new IOException(String.format("File not found %s ", filename));
      }
      finally {
        if (fc != null) {
          fc.close();
        }
      }

      return nread;
    }
  }
}
