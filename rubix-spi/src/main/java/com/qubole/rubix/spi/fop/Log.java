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
 * <p>
 * <p>
 * NOTICE: THIS FILE HAS BEEN MODIFIED BY  Qubole Inc UNDER COMPLIANCE WITH THE APACHE 2.0 LICENCE FROM THE ORIGINAL WORK
 * OF https://github.com/DanielYWoo/fast-object-pool.
 */
package com.qubole.rubix.spi.fop;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Daniel
 */
public class Log
{
  private static Logger logger = Logger.getLogger("FOP");

  private Log()
  {
  }

  private static String getString(Throwable ex, Object... objects)
  {
    StringBuilder sb = new StringBuilder();
    for (Object object : objects) {
      sb.append(object);
    }
    sb.append(", ");
    StringWriter writer = new StringWriter();
    ex.printStackTrace(new PrintWriter(writer));
    sb.append(writer.toString());
    return sb.toString();
  }

  public static boolean isDebug()
  {
    return logger.isLoggable(Level.FINE);
  }

  private static String getString(Object... objects)
  {
    if (objects.length > 1) {
      StringBuilder sb = new StringBuilder();
      for (Object object : objects) {
        sb.append(object);
      }
      return sb.toString();
    }
    else {
      return objects[0].toString();
    }
  }

  public static void debug(Object... objects)
  {
    if (logger.isLoggable(Level.FINE)) {
      logger.fine(getString(objects));
    }
  }

  public static void info(Object... objects)
  {
    if (logger.isLoggable(Level.INFO)) {
      logger.info(getString(objects));
    }
  }

  public static void warn(Object... objects)
  {
    if (logger.isLoggable(Level.WARNING)) {
      logger.warning(getString(objects));
    }
  }

  public static void error(Object... objects)
  {
    if (logger.isLoggable(Level.SEVERE)) {
      logger.severe(getString(objects));
    }
  }

  public static void error(Exception ex, Object... objects)
  {
    if (logger.isLoggable(Level.SEVERE)) {
      logger.severe(getString(ex, objects));
    }
  }
}
