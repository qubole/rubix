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

import com.qubole.rubix.client.robotframework.driver.server.RequestServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;

public class RequestClient
{
  private static final Log log = LogFactory.getLog(RequestClient.class);

  private static final String SERVER_NAME = "ContainerRequestServer";

  public RequestClient()
  {
  }

  public Map<String, Double> clientGetCacheMetrics(String host, int port)
  {
    return runGetCacheMetrics(host, port);
  }

  public boolean clientReadData(
      String host,
      int port,
      String remotePath,
      long readStart,
      int length,
      long fileSize,
      long lastModified,
      int clusterType)
  {
    ReadDataRequestParams params = new ReadDataRequestParams(remotePath, readStart, length, fileSize, lastModified, clusterType);
    return runReadData(host, port, params);
  }

  private boolean runReadData(String host, int port, ReadDataRequestParams params)
  {
    System.setProperty("sun.rmi.transport.tcp.responseTimeout", "60000");

    boolean dataRead = false;
    // initializeSecurityManager();
    try {
      System.out.println("1. Getting access to server");
      RequestServer containerDriver = getRequestServer(host, port);
      System.out.println("2. Executing read request");
      dataRead = containerDriver.executeReadDataRequest(new ReadDataRequest(), params);
      System.out.println("3. Returning result");
      System.out.println("Data read? " + dataRead);
    }
    catch (RemoteException | MalformedURLException | NotBoundException e) {
      System.err.println("RequestClient exception:");
      e.printStackTrace();
    }
    return dataRead;
  }

  public boolean clientReadDataFileSystem(
      String host,
      int port,
      String remotePath,
      long readStart,
      int length,
      long fileSize,
      long lastModified,
      int clusterType)
  {
    ReadDataRequestParams params = new ReadDataRequestParams(remotePath, readStart, length, fileSize, lastModified, clusterType);
    return runReadDataCFS(host, port, params);
  }

  private boolean runReadDataCFS(String host, int port, ReadDataRequestParams params)
  {
    System.setProperty("sun.rmi.transport.tcp.responseTimeout", "60000");

    boolean dataRead = false;
    // initializeSecurityManager();
    try {
      System.out.println("1. Getting access to server");
      RequestServer containerDriver = getRequestServer(host, port);
      System.out.println("2. Executing read request");
      dataRead = containerDriver.executeReadDataRequestWithCFS(new ReadDataCFSRequest(), params);
      System.out.println("3. Returning result");
      System.out.println("Data read? " + dataRead);
    }
    catch (RemoteException | MalformedURLException | NotBoundException e) {
      System.err.println("RequestClient exception:");
      e.printStackTrace();
    }
    return dataRead;
  }

  private static Map<String, Double> runGetCacheMetrics(String host, int port)
  {
    Map<String, Double> metrics = new HashMap<>();

    System.setProperty("java.security.policy", "/Users/jordanw/Development/Projects/Qubole/RubiX/rubix-tests/src/test/robotframework/acceptance/multi-node/docker/rs.policy");
    System.setProperty("sun.rmi.transport.tcp.responseTimeout", "60000");

    // initializeSecurityManager();
    try {
      System.out.println("1. Getting access to server");
      RequestServer containerDriver = getRequestServer(host, port);
      System.out.println("2. Executing request");
      metrics = containerDriver.executeGetCacheMetricsRequest(new GetCacheMetricsRequest());
      System.out.println("3. Returning results");
      System.out.println(metrics.toString());
    }
    catch (RemoteException | MalformedURLException | NotBoundException e) {
      System.err.println("RequestClient exception:");
      e.printStackTrace();
    }
    return metrics;
  }

  private static RequestServer getRequestServer(String host, int port) throws RemoteException, NotBoundException, MalformedURLException
  {
    // Registry registry = LocateRegistry.getRegistry(host, 1234);
    System.out.println("1.1 Getting registry object");
    Registry registry = LocateRegistry.getRegistry(host, 1099);
    System.out.println("1.2 Looking up server");
    RequestServer containerServer = (RequestServer) registry.lookup(SERVER_NAME);
    // RequestServer containerServer = (RequestServer) Naming.lookup("//" + host + ":" + port + "/" + SERVER_NAME);
    System.out.println("1.3 Returning server object");
    return containerServer;
  }

  private static void initializeSecurityManager()
  {
    if (System.getSecurityManager() == null) {
      System.setSecurityManager(new SecurityManager());
    }
  }
}
