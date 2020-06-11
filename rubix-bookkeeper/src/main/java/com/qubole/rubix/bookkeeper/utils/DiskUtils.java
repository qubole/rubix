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

import static com.qubole.rubix.spi.utils.DataSizeUnits.BYTES;
import static com.qubole.rubix.spi.utils.DataSizeUnits.KILOBYTES;

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
      long cacheDirSize = getDirectorySizeInMB(new File(diskMap.get(disk) + cacheDirSuffix));
      if (cacheDirSize == -1) {
        return -1;
      }
      cacheSize += cacheDirSize;
    }
    return (int) cacheSize;
  }

  /**
   * Gets the actual size occupied on the disk, for the given directory using du command.
   *
   * @return The size of the cache in MB.
   */
  public static long getDirectorySizeInMB(File dirname)
  {
    String cmd = "du -s " + dirname.toString();
    StringBuffer output = new StringBuffer();

    try {
      Process p;
      String[] env = new String[] {"BLOCKSIZE=1024"};
      p = Runtime.getRuntime().exec(cmd, env);
      p.waitFor();
      BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String line = "";
      while ((line = reader.readLine()) != null) {
        output.append(line + "\n");
      }
    }
    catch (Exception e) {
      log.error(String.format("Exception while calculating the size of the folder %s with exception : %s", dirname.toString(), e.toString()));
    }
    try {
      return KILOBYTES.toMB(Long.parseLong(output.toString().split("\\s+")[0]));
    }
    catch (NumberFormatException e)
    {
      log.warn(String.format("Unable to calculate size of directory %s, du output returned: %s", dirname, output.toString()));
      return -1L;
    }
  }
}
