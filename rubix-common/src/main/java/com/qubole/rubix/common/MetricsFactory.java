/**
 * Copyright (c) 2016. Qubole Inc
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
package com.qubole.rubix.common;

import com.qubole.rubix.spi.CacheConfig;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.Constructor;

/**
 * Created by Abhishek on 2/14/18.
 */
public final class MetricsFactory
{
  private static volatile Metrics metrics;

  private MetricsFactory()
  {
  }

  public static synchronized void init(Configuration conf) throws Exception
  {
    if (metrics == null) {
      Class meticsClass = conf.getClassByName(CacheConfig.getMetricsClass(conf));
      Constructor constructor = meticsClass.getConstructor(Configuration.class);
      metrics = (Metrics) constructor.newInstance(conf);
    }
  }

  public static Metrics getInstance()
  {
    return metrics;
  }

  public static synchronized void close() throws Exception
  {
    if (metrics != null) {
      metrics.close();
      metrics = null;
    }
  }
}
