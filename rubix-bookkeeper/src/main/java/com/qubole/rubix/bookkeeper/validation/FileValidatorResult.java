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
package com.qubole.rubix.bookkeeper.validation;

import java.util.HashSet;
import java.util.Set;

public class FileValidatorResult
{
  private int successes;
  private int totalFiles;
  private double successRate;
  private Set<String> filesWithoutMD = new HashSet<>();
  private Set<String> corruptedCachedFiles = new HashSet<>();
  private Set<String> untrackedCachedFiles = new HashSet<>();

  public FileValidatorResult()
  {
  }

  public FileValidatorResult(int successes, int totalFiles, Set<String> filesWithoutMD,
                             Set<String> corruptedCachedFiles, Set<String> untrackedCachedFiles)
  {
    this.successes = successes;
    this.totalFiles = totalFiles;
    this.filesWithoutMD = filesWithoutMD;
    this.corruptedCachedFiles = corruptedCachedFiles;
    this.untrackedCachedFiles = untrackedCachedFiles;

    this.successRate = totalFiles > 0 ? ((double) successes / (double) totalFiles) : 0;
  }

  public int getSuccessCount()
  {
    return successes;
  }

  public int getFailureCount()
  {
    return totalFiles - successes;
  }

  public int getTotalFiles()
  {
    return totalFiles;
  }

  public double getSuccessRate()
  {
    return successRate;
  }

  public Set<String> getFilesWithoutMD()
  {
    return filesWithoutMD;
  }

  public Set<String> getCorruptedCachedFiles()
  {
    return corruptedCachedFiles;
  }

  public Set<String> getUntrackedCachedFiles()
  {
    return untrackedCachedFiles;
  }

  public void addResult(FileValidatorResult result)
  {
    this.successes += result.successes;
    this.totalFiles += result.totalFiles;
    filesWithoutMD.addAll(result.filesWithoutMD);
    corruptedCachedFiles.addAll(result.corruptedCachedFiles);
    untrackedCachedFiles.addAll(result.untrackedCachedFiles);

    this.successRate = (double) successes / (double) totalFiles;
  }
}
