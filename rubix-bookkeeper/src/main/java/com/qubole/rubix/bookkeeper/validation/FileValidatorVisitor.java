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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Set;

public class FileValidatorVisitor extends SimpleFileVisitor<Path>
{
  private static final Log log = LogFactory.getLog(FileValidatorVisitor.class);
  private static String metadataFileSuffix;

  private final Configuration conf;

  private int successes;
  private int totalCacheFiles;
  private final Set<String> filesWithoutMd = new HashSet<>();

  public FileValidatorVisitor(Configuration conf)
  {
    this.conf = conf;

    metadataFileSuffix = CacheConfig.getCacheMetadataFileSuffix(conf);
  }

  @Override
  public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
  {
    if (!CacheUtil.isMetadataFile(file.toString(), conf)) {
      totalCacheFiles++;
      String fileName = file.toString();
      int generationNumber = Integer.parseInt(fileName.substring(fileName.indexOf("_g") + 2));
      Path mdFile = file.resolveSibling(file.getFileName() + metadataFileSuffix + generationNumber);
      if (Files.exists(mdFile)) {
        successes++;
      }
      else {
        filesWithoutMd.add(file.toString());
      }
    }

    return super.visitFile(file, attrs);
  }

  /**
   * Get the current cache validation result.
   *
   * @return The result of the cache validation.
   */
  public FileValidatorResult getResult()
  {
    return new FileValidatorResult(successes, totalCacheFiles, filesWithoutMd);
  }
}
