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
package com.qubole.rubix.client.robotframework.driver.client;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.io.Serializable;

public class ReadDataCFSRequest implements Serializable
{
  public static final long serialVersionUID = 126L;

  public boolean execute(FSDataInputStream inputStream, ReadDataRequestParams params)
  {
    System.out.println("Reading data");

    try {
      final int readSize = inputStream.read(
          new byte[params.getLength()],
          (int) params.getReadStart(),
          params.getLength());
      return readSize == params.getLength();
    }
    catch (IOException ex) {
      System.err.println("Could not read data: " + ex.toString());
    }
    return false;
  }
}
