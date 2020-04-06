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
package com.qubole.rubix.spi.client;

import com.qubole.rubix.spi.thrift.Request;
import com.qubole.rubix.spi.thrift.TestingService;
import org.apache.thrift.TException;

public class Processor
    implements TestingService.Iface
{
  @Override
  public String echo(Request request)
      throws TException
  {
    try {
      Thread.sleep(request.sleepSecs * 1000);
      return request.getMessage();
    }
    catch (InterruptedException e) {
      throw new TException(e);
    }
  }
}
