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

package com.qubole.rubix.bookkeeper.test;

import com.google.common.base.Joiner;
import com.qubole.rubix.core.utils.DeleteFileVisitor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public final class BookKeeperTestUtils
{
  private BookKeeperTestUtils()
  {
  }

  /**
   * Get the name of a temporary directory to be used for unit testing.
   *
   * @param testSubdirectoryName  The name of the subdirectory to be used for testing.
   * @return The path name of the cache directory to be used for testing.
   */
  public static String getTestCacheDirPrefix(String testSubdirectoryName)
  {
    return Joiner.on(File.separator).join(System.getProperty("java.io.tmpdir"), testSubdirectoryName);
  }

  /**
   * Create the parent directories necessary for cache directory creation.
   *
   * @param cacheDirPrefix  The path prefix for the directories to be added.
   * @param maxDisks        The maximum number of parent directories to create.
   * @throws IOException if an I/O error occurs while creating directories.
   */
  public static void createCacheParentDirectories(String cacheDirPrefix, int maxDisks) throws IOException
  {
    for (int i = 0; i < maxDisks; i++) {
      Files.createDirectories(Paths.get(cacheDirPrefix + i));
    }
  }

  /**
   * Remove all cache directories and their parents.
   *
   * @param cacheDirPrefix  The path prefix for the directories to be removed.
   * @throws IOException if an I/O error occurs while deleting directories.
   */
  public static void removeCacheParentDirectories(String cacheDirPrefix, int maxDisks) throws IOException
  {
    for (int i = 0; i < maxDisks; i++) {
      Files.walkFileTree(Paths.get(cacheDirPrefix + i), new DeleteFileVisitor());
    }
  }
}
