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

package com.qubole.rubix.health;

import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.RetryingBookkeeperClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

/**
* Created by kvankayala on 16 Dec 2018.
*/

public class BookKeeperHealth extends Configured
{
  private static Configuration conf = new Configuration();
  private static final Log log = LogFactory.getLog(BookKeeperHealth.class);
  private static BookKeeperFactory factory;

  public BookKeeperHealth(Configuration conf, BookKeeperFactory factory)
  {
    this.conf = conf;
    this.factory = factory;
  }

  public BookKeeperHealth(Configuration conf)
  {
    this(conf, new BookKeeperFactory());
  }

  public static void main(String[]args)
  {
    BookKeeperHealth bookkeeperhealth = new BookKeeperHealth(conf);
    boolean isBookKeeperAlive = bookkeeperhealth.checkIfBookKeeperAlive();
    if (isBookKeeperAlive == false) {
      Runtime.getRuntime().exit(1);
    }
    else {
      Runtime.getRuntime().exit(0);
    }
  }

  public boolean checkIfBookKeeperAlive()
  {
    boolean isBksAlive = false;
    try (RetryingBookkeeperClient rclient = this.factory.createBookKeeperClient(conf)) {
      isBksAlive = rclient.isBookKeeperAlive();
    }
    catch (Exception e) {
      log.error("Bookkeeper is not responding", e);
    }
    return isBksAlive;
  }
}
