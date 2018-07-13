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

  public static String getTestCacheDirPrefix(String testSubdirectoryName)
  {
    return Joiner.on(File.separator).join(System.getProperty("java.io.tmpdir"), testSubdirectoryName);
  }

  public static void createCacheParentDirectories(String cacheDirPrefix, int maxDisks) throws IOException
  {
    Files.createDirectories(Paths.get(cacheDirPrefix));
    for (int i = 0; i < maxDisks; i++) {
      Files.createDirectories(Paths.get(cacheDirPrefix + i));
    }
  }

  public static void removeCacheParentDirectories(String cacheDirPrefix) throws IOException
  {
    Files.walkFileTree(Paths.get(cacheDirPrefix), new DeleteFileVisitor());
    Files.deleteIfExists(Paths.get(cacheDirPrefix));
  }
}
