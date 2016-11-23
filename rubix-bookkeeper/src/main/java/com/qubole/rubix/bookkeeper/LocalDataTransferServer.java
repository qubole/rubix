/**
 * Copyright (c) 2016. Qubole Inc
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

import com.google.common.base.Throwables;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.DataTransferClientHelper;
import com.qubole.rubix.spi.DataTransferHeader;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by sakshia on 26/10/16.
 */

public class LocalDataTransferServer
{
    private static Log log = LogFactory.getLog(LocalDataTransferServer.class.getName());
    private static Configuration conf;
    private static  LocalServer localServer;

    private LocalDataTransferServer()
    {
    }

    public static void main(String[] args)
    {
        conf = new Configuration();
        startServer(conf);
    }

    public static void startServer(Configuration conf)
    {
        localServer = new LocalServer(conf);
        new Thread(localServer).run();
    }

    public static void stopServer()
    {
        localServer.stop();
    }

    public static class LocalServer implements Runnable
    {
        static ServerSocketChannel listener;
        Configuration conf;

        public LocalServer(Configuration conf)
        {
            this.conf = conf;
        }

        @Override
        public void run()
        {
            int port = CacheConfig.getLocalServerPort(conf);

            ExecutorService threadPool = Executors.newCachedThreadPool();
            try {
                listener = ServerSocketChannel.open();
                listener.bind(new InetSocketAddress(port));
                log.info("Listening on port " + port);
                while (true) {
                    SocketChannel clientSocket = listener.accept();
                    ClientServiceThread cliThread = new ClientServiceThread(clientSocket);
                    threadPool.execute(cliThread);
                }
            }
            catch (AsynchronousCloseException e) {
                log.info("Stopping Local Transfer server");
            }
            catch (IOException e) {
                log.error(String.format("Error starting Local Transfer server %s", Throwables.getStackTraceAsString(e)));
            }

        }

        public void stop()
        {
            try {
                listener.close();
            }
            catch (IOException e) {
                log.error(String.format("Error stopping Local Transfer server %s", Throwables.getStackTraceAsString(e)));
            }
        }

    }

    static class ClientServiceThread
            extends Thread
    {
        SocketChannel localDataTransferClient;
        RetryingBookkeeperClient bookKeeperClient;

        ClientServiceThread(SocketChannel s)
        {
            localDataTransferClient = s;
        }

        public void run()
        {
            try {
                log.debug("Connected to node - " + localDataTransferClient.getLocalAddress());
                BookKeeperFactory bookKeeperFactory = new BookKeeperFactory();
                ByteBuffer dataInfo = ByteBuffer.allocate(CacheConfig.getDataTransferBufferSize(conf));

                int read = localDataTransferClient.read(dataInfo);
                if (read == -1) {
                    throw new Exception("Could not read data from Non-local node");
                }
                dataInfo.flip();

                DataTransferHeader header = DataTransferClientHelper.readHeaders(dataInfo);
                long offset = header.getOffset();
                int readLength = header.getReadLength();
                String remotePath = header.getFilePath();

                bookKeeperClient = bookKeeperFactory.createBookKeeperClient(conf);
                if (!bookKeeperClient.readData(remotePath, offset, readLength, header.getFileSize(), header.getLastModified(), header.getClusterType())) {
                    throw new Exception("Could not cache data required by non-local node");
                }

                String filename = CacheConfig.getLocalPath(remotePath, conf);
                FileChannel fc = new FileInputStream(filename).getChannel();
                int maxCount = CacheConfig.getBufferSize(conf);
                int lengthRemaining = readLength;
                long position = offset;
                int nread = 0;
                while (nread < readLength) {
                    if (maxCount > lengthRemaining) {
                        maxCount = lengthRemaining;
                    }
                    nread += fc.transferTo(position + nread, maxCount, localDataTransferClient);
                    lengthRemaining = readLength - nread;
                }
                if (bookKeeperClient != null) {
                    bookKeeperClient.close();
                }
                fc.close();
            }
            catch (Exception e) {
                log.info("Error in Local Data Transfer Server: " + Throwables.getStackTraceAsString(e));
                return;
            }
            finally {
                try {
                    localDataTransferClient.close();
                }
                catch (IOException e) {
                    log.info("Error in Local Data Transfer Server: " + Throwables.getStackTraceAsString(e));
                }
            }
        }
    }
}
