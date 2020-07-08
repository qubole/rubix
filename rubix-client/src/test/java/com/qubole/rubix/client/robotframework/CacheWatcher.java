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
package com.qubole.rubix.client.robotframework;

import com.qubole.rubix.spi.CacheUtil;
import com.sun.nio.file.SensitivityWatchEventModifier;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.qubole.rubix.spi.CacheUtil.UNKONWN_GENERATION_NUMBER;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

public class CacheWatcher
{
  private static final Log log = LogFactory.getLog(CacheWatcher.class);
  private static final int MAX_RETRIES = 2;

  private final WatchService watcher;
  private final Map<WatchKey, Path> watchKeys = new HashMap<>();
  private final Configuration conf;
  private final int maxWaitTime;

  public CacheWatcher(Configuration conf, Path cacheDir, int maxWaitTime) throws IOException
  {
    PropertyConfigurator.configure("rubix-client/src/test/resources/log4j.properties");
    log.info("=======  New Watcher  =======");

    this.watcher = FileSystems.getDefault().newWatchService();
    this.maxWaitTime = maxWaitTime;
    this.conf = conf;

    registerAll(cacheDir);
  }

  public boolean watchForCacheFiles(List<TestClientReadRequest> requests)
  {
    for (int retries = 0; retries < MAX_RETRIES; ) {
      log.info("Watching for cache files...");

      WatchKey key;
      try {
        key = watcher.poll(maxWaitTime, TimeUnit.MILLISECONDS);
        if (key == null) {
          log.error(String.format("No events! (waited %d ms)", maxWaitTime));
          log.info("Doing one final check");
          return areAllFilesCached(requests);
        }
      }
      catch (InterruptedException e) {
        log.error("Polling interrupted", e);
        log.info("Retrying...");
        retries++;
        continue;
      }

      Path cacheDir = watchKeys.get(key);
      if (cacheDir == null) {
        log.error("Found an invalid key!");
        log.info("Retrying...");
        retries++;
        continue;
      }

      log.info("Checking cache files");
      if (areAllFilesCached(requests)) {
        return true;
      }
      else {
        continue;
      }
    }

    log.error("Ran out of retries");
    return false;
  }

  private boolean areAllFilesCached(List<TestClientReadRequest> requests)
  {
    Map<Path, Boolean> fileStatus = getCacheFileStatus(requests);
    if (fileStatus.containsValue(false)) {
      log.warn("Some files have not been fully cached");
      for (Path cacheFile : fileStatus.keySet()) {
        log.warn(String.format("* %s [%s]", cacheFile.toString(), getFileStatusString(cacheFile)));
      }
      return false;
    }
    else {
      return true;
    }
  }

  private Map<Path, Boolean> getCacheFileStatus(List<TestClientReadRequest> requests)
  {
    Map<Path, Boolean> fileCacheStatus = new HashMap<>();

    for (TestClientReadRequest request : requests) {
      Path cacheFile = Paths.get(CacheUtil.getLocalPath(request.getRemotePath(), conf, UNKONWN_GENERATION_NUMBER + 1));
      int expectedSize = request.getReadLength();

      if (Files.exists(cacheFile)) {
        if (isCacheFileSizeCorrect(cacheFile, expectedSize)) {
          log.info(String.format("File has expected size! %s", cacheFile.toString()));
          fileCacheStatus.put(cacheFile, true);
        }
        else {
          log.info(String.format("File created! %s", cacheFile.toString()));
        }
      }
    }

    return fileCacheStatus;
  }

  private boolean isCacheFileSizeCorrect(Path cacheFileName, long expectedSize)
  {
    try {
      return Files.size(cacheFileName) == expectedSize;
    }
    catch (IOException e) {
      log.error(String.format("Error getting size for file: %s", cacheFileName), e);
    }
    return false;
  }

  private String getFileStatusString(Path cacheFile)
  {
    if (Files.exists(cacheFile)) {
      try {
        return String.format("CREATED (%d B)", Files.size(cacheFile));
      }
      catch (IOException e) {
        log.error(String.format("Error getting size for file: %s", cacheFile.toString()), e);
        return "ERRORED";
      }
    }
    else {
      return "PENDING";
    }
  }

  private void registerAll(Path cacheParentDir) throws IOException
  {
    Files.walkFileTree(cacheParentDir, new SimpleFileVisitor<Path>()
    {
      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
          throws IOException
      {
        register(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  private void register(Path cacheDir) throws IOException
  {
    WatchKey cacheDirKey = cacheDir.register(
        watcher,
        new WatchEvent.Kind[]{ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE, OVERFLOW},
        SensitivityWatchEventModifier.HIGH);

    watchKeys.put(cacheDirKey, cacheDir);
    log.debug("* Registered " + cacheDir);
  }
}
