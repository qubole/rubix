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

import java.util.Set;

public class ValidationResult
{
  private int successes;
  private int totalFiles;
  private double successRate;
  private Set<String> filesWithoutMD;

  public ValidationResult(int successes, int totalFiles, Set<String> filesWithoutMD)
  {
    this.successes = successes;
    this.totalFiles = totalFiles;
    this.filesWithoutMD = filesWithoutMD;

    this.successRate = totalFiles > 0 ? ((double) successes / (double) totalFiles) : 0;
  }

  public int getSuccesses()
  {
    return successes;
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

  public void addResult(ValidationResult result)
  {
    this.successes += result.successes;
    this.totalFiles += result.totalFiles;
    filesWithoutMD.addAll(result.filesWithoutMD);

    this.successRate = (double) successes / (double) totalFiles;
  }
}
