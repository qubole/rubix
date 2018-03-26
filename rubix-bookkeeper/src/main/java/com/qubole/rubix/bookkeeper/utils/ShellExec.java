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
package com.qubole.rubix.bookkeeper.utils;

import com.google.common.io.ByteStreams;
//import io.airlift.log.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ShellExec
{
  //private static final Logger log = Logger.get(ShellExec.class);

  private List<String> args;

  public static class CommandResult
  {
    private int exitValue;
    private String out;
    private String err;

    public int getExitValue()
    {
      return exitValue;
    }

    public void setExitValue(int exitValue)
    {
      this.exitValue = exitValue;
    }

    public String getOut()
    {
      return out;
    }

    public void setOut(String out)
    {
      this.out = out;
    }

    public String getErr()
    {
      return err;
    }

    public void setErr(String err)
    {
      this.err = err;
    }
  }

  public ShellExec(String cmd)
  {
    super();
    this.args = Arrays.asList("/bin/bash", "-c", cmd);
  }

  public CommandResult runCmd() throws IOException
  {
    checkNotNull(args);
    checkArgument(args.size() > 0);
    //log.debug("executing " + args);
    CommandResult cr = new CommandResult();
    Process p = null;
    try {
      ProcessBuilder pb = new ProcessBuilder(args);
      p = pb.start();
    }
    catch (IOException e) {
      cr.setExitValue(1);
      cr.setErr("unable to start process");
      throw e;
    }

    ThreadedStreamHandler out = new ThreadedStreamHandler(
        p.getInputStream());
    ThreadedStreamHandler err = new ThreadedStreamHandler(
        p.getErrorStream());

    out.start();
    err.start();

    int exitVal = 1;
    while (true) {
      try {
        exitVal = p.waitFor();
        out.join();
        err.join();
        break;
      }
      catch (InterruptedException e) {
        // ignore
      }
    }
    cr.setExitValue(exitVal);
    cr.setOut(out.toString());
    cr.setErr(err.toString());
    return cr;
  }

  public static class ThreadedStreamHandler
      extends Thread
  {
    private InputStream is;
    private String str;

    public ThreadedStreamHandler(InputStream is)
    {
      this.is = is;
      setDaemon(true);
    }

    private void close()
    {
      try {
        is.close();
      }
      catch (IOException e) {
        str = "unable to close " + e.getMessage();
      }
    }

    @Override
    public void run()
    {
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        ByteStreams.copy(is, ps);
        str = baos.toString();
      }
      catch (IOException e) {
        str = e.toString();
      }
      finally {
        close();
      }
    }

    @Override
    public String toString()
    {
      return str;
    }
  }
}
