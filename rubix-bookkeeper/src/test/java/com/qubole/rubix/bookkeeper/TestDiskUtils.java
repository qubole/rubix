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
package com.qubole.rubix.bookkeeper;
import com.qubole.rubix.bookkeeper.utils.DiskUtils;
import com.qubole.rubix.common.utils.DeleteFileVisitor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.testng.Assert.assertTrue;

/**
 * Created by kvankayala on 21/1/19.
 */

public class TestDiskUtils
{
  private static Log log = LogFactory.getLog(TestDiskUtils.class);
  private static final String testDirectory = System.getProperty("java.io.tmpdir") + "/TestDiskUtils/";

  @BeforeMethod
  public void setupMethod() throws IOException
  {
    Files.createDirectories(Paths.get(testDirectory));
  }
  @AfterMethod
  public void teadDownMethod() throws IOException
  {
    Files.walkFileTree(Paths.get(testDirectory), new DeleteFileVisitor());
    Files.deleteIfExists(Paths.get(testDirectory));
  }
  @Test
  public void testGetCacheSizeMB() throws IOException
  {
    File file = new File(testDirectory + "testfile");
    log.info("Name of the File : " + file.toString());
    FileWriter fileWriter = new FileWriter(file.toString());
    for (int i = 0; i < 10000000; i++) {
      fileWriter.write('a' + i % 10);
    }
    File dirName = new File(testDirectory);
    long dsize = DiskUtils.getDirSizeMB(dirName);
    assertTrue(dsize == 9, "DiskSize is reported :" + dsize + " but expected : 9");
    file.deleteOnExit();
  }

  @Test
  public void testGetCacheSizeMB_Rafile() throws IOException
  {
    String fileName = testDirectory + "testfile";
    File dirName = new File(testDirectory);
    RandomAccessFile rafile = new RandomAccessFile(fileName, "rw");
    rafile.seek(20000000);
    for (int i = 0; i < 1150000; i++) {
      rafile.writeChar('a' + i % 10);
    }
    rafile.close();
    File file = new File(fileName);
    long dsize = DiskUtils.getDirSizeMB(dirName);
    assertTrue(dsize == 3, "DiskSize is reported :" + dsize + " but expected : 2");
    file.deleteOnExit();
  }
}
