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
package com.qubole.rubix.spi;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Created by sakshia on 21/11/16.
 */
public class DataTransferClientHelper
{
  private DataTransferClientHelper()
  {
  }

  public static SocketChannel createDataTransferClient(String remoteNodeName, Configuration conf)
      throws IOException
  {
    SocketAddress sad = new InetSocketAddress(remoteNodeName, CacheConfig.getLocalServerPort(conf));
    SocketChannel sc = SocketChannel.open();
    sc.socket().setSoTimeout(CacheConfig.getSocketReadTimeOutDefault(conf));
    sc.configureBlocking(true);
    sc.socket().connect(sad, CacheConfig.getClientTimeout(conf));
    return sc;
  }

    /* order is:
    int : filePathLength
    String : filePath
    long : offset
    int : readLength
    long : fileSize
    long : lastModified
    int : clusterType */

  public static ByteBuffer writeHeaders(Configuration conf, DataTransferHeader header)
  {
    ByteBuffer buf = ByteBuffer.allocate(CacheConfig.getMaxHeaderSize(conf));
    buf.putInt(header.getFilePath().length());
    buf.put(header.getFilePath().getBytes());
    buf.putLong(header.getOffset());
    buf.putInt(header.getReadLength());
    buf.putLong(header.getFileSize());
    buf.putLong(header.getLastModified());
    buf.putInt(header.getClusterType());
    buf.flip();
    return buf;
  }

  public static DataTransferHeader readHeaders(ByteBuffer dataInfo)
  {
    byte[] fileBytes = new byte[dataInfo.getInt()];
    dataInfo.get(fileBytes);
    String remotePath = new String(fileBytes);
    return new DataTransferHeader(dataInfo.getLong(), dataInfo.getInt(), dataInfo.getLong(),
        dataInfo.getLong(), dataInfo.getInt(), remotePath);
  }
}
