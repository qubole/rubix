/**
 * Copyright (c) 2018. Qubole Inc
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
package com.qubole.rubix.bookkeeper.utils;

import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * Created by stagra on 29/1/16.
 */
public class DiskUtils
{
  private static Log log = LogFactory.getLog(DiskUtils.class.getName());

  private DiskUtils()
  {
    //
  }

  /**
   * Convert bytes to MB.
   *
   * @param bytes   The number of bytes.
   * @return the number of bytes as MB.
   */
  public static long bytesToMB(long bytes)
  {
    return (bytes / 1024 / 1024);
  }

  // Return actual size of a sparse file in bytes
  public static long getActualSize(String path)
      throws IOException
  {
    File file = new File(path);
    String cmd = "ls -s " + path + " | cut -d ' ' -f 1";
    ShellExec se = new ShellExec(cmd);
    log.debug("Running: " + cmd);
    ShellExec.CommandResult cr = se.runCmd();
    long size = Long.parseLong(cr.getOut().trim());
    return size * 1024;
  }

  public static void clearDirectory(String path) throws IOException
  {
    String cmd = "rm -rf " + path;
    ShellExec se = new ShellExec(cmd);
    log.debug("Running: " + cmd);
    ShellExec.CommandResult cr = se.runCmd();
  }

  /**
   * Get the current size of the data cached to this system.
   *
   * @return The size of the cache in MB.
   */
  public static int getCacheSizeMB(Configuration conf)
  {
    final Map<Integer, String> diskMap = CacheUtil.getCacheDiskPathsMap(conf);
    final String cacheDirSuffix = CacheConfig.getCacheDataDirSuffix(conf);

    long cacheSize = 0;
    for (int disk = 0; disk < diskMap.size(); disk++) {
      long cacheDirSize = getDirSizeMB(new File(diskMap.get(disk) + cacheDirSuffix));
      cacheSize += cacheDirSize;
    }
    return (int) cacheSize;
  }

  /**
   * Gets the actual size occupied on the disk, for the given directory using du command.
   *
   * @return The size of the cache in MB.
   */
  public static long getDirSizeMB(File dirname)
  {
    String cmd = "du -s " + dirname.toString();
    StringBuffer output = new StringBuffer();
    Process p;
    long dirSize;
    try {
      p = Runtime.getRuntime().exec(cmd);
      p.waitFor();
      BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String line = "";
      while ((line = reader.readLine()) != null) {
        output.append(line + "\n");
      }
    }
    catch (Exception e) {
      log.error("Exception while calculating the size of the folder " + dirname.toString());
    }
    dirSize = Long.parseLong(output.toString().split("\\s+")[0]) / 1024;
    return dirSize;
  }

  public static int getUsedSpaceMB(Configuration conf)
  {
    long used = 0;
    for (int d = 0; d < CacheUtil.getCacheDiskCount(conf); d++) {
      File localPath = new File(CacheUtil.getDirPath(d, conf));
      used += localPath.getTotalSpace() - localPath.getUsableSpace();
    }
    return (int) bytesToMB(used);
  }
}
