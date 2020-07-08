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
package com.qubole.rubix.bookkeeper.validation;

import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ValidatorFileGen
{
  private static final Log log = LogFactory.getLog(ValidatorFileGen.class);

  private static final int DIRECTORY_NAME_LENGTH = 15;
  private static final int FILE_NAME_LENGTH = 10;

  private ValidatorFileGen()
  {
  }

  /**
   * Generate files used to test the {@link FileValidatorVisitor}.
   *
   * @param conf    The current Hadoop configuration.
   * @param depth   The depth to which directories will be created.
   * @param dirs    The number of directories to create at each depth.
   * @param files   The number of files to create at each depth.
   * @param mdStep  The frequency at which to generate metadata files.
   * @return The result of the file generation.
   * @throws IOException if an I/O error occurs while creating files.
   */
  public static FileGenResult generateTestFiles(Configuration conf, int depth, int dirs, int files, int mdStep) throws IOException
  {
    CacheUtil.createCacheDirectories(conf);
    final Map<Integer, String> diskMap = CacheUtil.getCacheDiskPathsMap(conf);

    FileGenResult result = new FileGenResult(0, 0, new HashSet<String>());
    for (int disk = 0; disk < diskMap.size(); disk++) {
      result.addResult(createCacheSubDirectories(conf, Paths.get(diskMap.get(disk)), 1, depth, dirs, files, mdStep));
    }

    return result;
  }

  /**
   * Create a subdirectory for cache files.
   *
   * @param conf            The current Hadoop configuration.
   * @param currentDirPath  The path to the current directory.
   * @param currentDepth    The current depth of directory generation.
   * @param maxDepth        The maximum depth of directory generation.
   * @param dirs            The number of directories to create.
   * @param files           The number of files to create.
   * @param mdStep          The frequency at which to generate metadata files.
   * @return The result of the directory generation.
   * @throws IOException if an I/O error occurs while creating directories.
   */
  private static FileGenResult createCacheSubDirectories(Configuration conf, Path currentDirPath, int currentDepth, int maxDepth, int dirs, int files, int mdStep) throws IOException
  {
    if (currentDepth == maxDepth) {
      return createCacheFiles(conf, currentDirPath, files, mdStep);
    }

    final FileGenResult result = new FileGenResult(0, 0, new HashSet<String>());
    for (int dirNum = 0; dirNum < dirs; dirNum++) {
      String subDirName = RandomStringUtils.random(DIRECTORY_NAME_LENGTH, true, true);
      Path subDirPath = currentDirPath.resolve(subDirName);

      Files.createDirectory(subDirPath);

      result.addResult(createCacheSubDirectories(conf, subDirPath, currentDepth + 1, maxDepth, dirs, files, mdStep));
      result.addResult(createCacheFiles(conf, subDirPath, files, mdStep));
    }

    return result;
  }

  /**
   * Create test files in the given directory.
   *
   * @param conf            The current Hadoop configuration.
   * @param parentDirPath   The directory in which to create the cache files.
   * @param files           The number of cache files to create.
   * @param mdStep          The frequency at which to generate metadata files.
   * @return The result of the directory generation.
   * @throws IOException if an I/O error occurs while creating files.
   */
  private static FileGenResult createCacheFiles(Configuration conf, Path parentDirPath, int files, int mdStep) throws IOException
  {
    int filesCreated = 0;
    int mdFilesCreated = 0;
    final Set<String> filesWithoutMD = new HashSet<>();
    final String mdFileSuffix = CacheConfig.getCacheMetadataFileSuffix(conf);

    for (int fileNum = 0; fileNum < files; fileNum++) {
      String fileName = RandomStringUtils.random(FILE_NAME_LENGTH, true, true) + "_g" + fileNum;
      Path filePath = parentDirPath.resolve(fileName);
      Files.createFile(filePath);

      if ((fileNum % mdStep == 0)) {
        Path mdFilePath = filePath.resolveSibling(filePath.getFileName() + mdFileSuffix + fileNum);
        Files.createFile(mdFilePath);
        mdFilesCreated++;
      }
      else {
        filesWithoutMD.add(filePath.toString());
      }

      filesCreated++;
    }
    return new FileGenResult(filesCreated, mdFilesCreated, filesWithoutMD);
  }

  /**
   * Class to store the results of file generation for test verification.
   */
  public static class FileGenResult
  {
    private int totalCacheFilesCreated;
    private int totalMDFilesCreated;
    private Set<String> filesWithoutMd;

    public FileGenResult(int totalCacheFilesCreated, int totalMDFilesCreated, Set<String> filesWithoutMd)
    {
      this.totalCacheFilesCreated = totalCacheFilesCreated;
      this.totalMDFilesCreated = totalMDFilesCreated;
      this.filesWithoutMd = filesWithoutMd;
    }

    public int getTotalCacheFilesCreated()
    {
      return totalCacheFilesCreated;
    }

    public int getTotalMDFilesCreated()
    {
      return totalMDFilesCreated;
    }

    public Set<String> getFilesWithoutMd()
    {
      return filesWithoutMd;
    }

    public double getSuccessRate()
    {
      return ((double) totalMDFilesCreated / (double) totalCacheFilesCreated);
    }

    public void addResult(FileGenResult result)
    {
      this.totalCacheFilesCreated += result.totalCacheFilesCreated;
      this.totalMDFilesCreated += result.totalMDFilesCreated;
      this.filesWithoutMd.addAll(result.filesWithoutMd);
    }
  }
}
