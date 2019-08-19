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
package com.qubole.rubix.client.robotframework.testdriver;

import java.io.Serializable;
import java.util.List;

public class Job implements Serializable
{
  private List<Task> tasks;
  private int cacheRequestRatio;
  private int remoteRequestRatio;
  private int nonLocalRequestRatio;
  private final int ratioScale;

  public Job(List<Task> tasks, int remoteRequestRatio, int cacheRequestRatio, int nonLocalRequestRatio)
  {
    int ratioSum = remoteRequestRatio + cacheRequestRatio + nonLocalRequestRatio;
    if (tasks.size() % (remoteRequestRatio + cacheRequestRatio + nonLocalRequestRatio) != 0) {
      throw new ArithmeticException("Task count should be a multiple of the sum of the request ratios.");
    }
    this.tasks = tasks;
    this.remoteRequestRatio = remoteRequestRatio;
    this.cacheRequestRatio = cacheRequestRatio;
    this.nonLocalRequestRatio = nonLocalRequestRatio;

    this.ratioScale = tasks.size() / ratioSum;
  }

  public List<Task> getTasks()
  {
    return tasks;
  }

  public int getRemoteRequestRatio()
  {
    return remoteRequestRatio;
  }

  public int getNumRemoteRequests()
  {
    return remoteRequestRatio * ratioScale;
  }

  public int getCacheRequestRatio()
  {
    return cacheRequestRatio;
  }

  public int getNumCacheRequests()
  {
    return cacheRequestRatio * ratioScale;
  }

  public int getNonLocalRequestRatio()
  {
    return nonLocalRequestRatio;
  }

  public int getNumNonLocalRequests()
  {
    return nonLocalRequestRatio * ratioScale;
  }

  @Override
  public String toString()
  {
    return String.format(
        "[Ratio] %dR : %dC : %dNL\nRubiX Job (Tasks: %s)",
        remoteRequestRatio,
        cacheRequestRatio,
        nonLocalRequestRatio,
        tasks.toString());
  }
}
