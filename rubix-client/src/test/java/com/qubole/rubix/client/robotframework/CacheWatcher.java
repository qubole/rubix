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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

public class CacheWatcher
{
  private static Log log = LogFactory.getLog(CacheWatcher.class);

  public static final WatchEvent.Kind[] watchEvents = new WatchEvent.Kind[]{ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE, OVERFLOW};

  private WatchService watcher;
  private Map<WatchKey, Path> watchKeys = new HashMap<>();
  private final boolean recursive;
  private final int maxWaitTime;
  private Configuration conf;

  public CacheWatcher(Configuration conf, Path cacheDir, boolean recursive, int maxWaitTime) throws IOException
  {
    // System.setProperty("log4j.configuration", "file:/Users/jordanw/Development/Projects/Qubole/RubiX/log4j_cache.properties");
    // System.out.println("Current directory: " + System.getProperty("user.dir"));
    PropertyConfigurator.configure("/Users/jordanw/Development/Projects/Qubole/RubiX/log4j_cache.properties");
    log.warn("=== ===  New Watcher  === ===");

    this.watcher = FileSystems.getDefault().newWatchService();
    this.recursive = recursive;
    this.maxWaitTime = maxWaitTime;
    this.conf = conf;

    if (recursive) {
      log.info(String.format("Scanning %s ...", cacheDir));
      registerAll(cacheDir);
      log.info("Done!");
    }
    else {
      register(cacheDir);
    }
  }

  public boolean processEvents(List<TestClientReadRequest> requests)
  {
    Map<String, Boolean> fileCacheStatus = new HashMap<>(requests.size());
    for (TestClientReadRequest request : requests) {
      fileCacheStatus.put(request.getRemotePath(), false);
    }

    log.info(String.format("Keys registered: %s", Arrays.toString(watchKeys.entrySet().toArray())));

    for (; ; ) {
      WatchKey key;
      try {
        log.info("Starting watcher poll");
        key = watcher.poll(maxWaitTime, TimeUnit.MILLISECONDS);
        // key = watcher.take();
        if (key == null) {
          log.error(String.format("No events! (waited %d ms)", maxWaitTime));
          break;
        }
        log.info("Got key");
      }
      catch (InterruptedException e) {
        log.error("Polling interrupted", e);
        return false;
      }

      Path cacheDir = watchKeys.get(key);
      if (cacheDir == null) {
        log.error("Key not recognized! Stopping watch");
        // continue;
        break;
      }

      for (WatchEvent<?> event : key.pollEvents()) {
        WatchEvent.Kind eventKind = event.kind();
        if (eventKind == OVERFLOW) {
          // TODO: Handle overflow
          continue;
        }

        @SuppressWarnings("unchecked")
        WatchEvent<Path> pathEvent = (WatchEvent<Path>) event;
        Path name = pathEvent.context();
        Path child = cacheDir.resolve(name);

        // kind: CREATED; DELETED; MODIFIED
        // CREATE: new file cached; new MD created
        // MODIFY: # of cached bytes changed [verify]
        // DELETE: cache file evicted, invalidated, expired
        // String expectedCacheFile = CacheUtil.getLocalPath(expectedFile, conf);

        // log.info(String.format("[%s] Path: %s", eventKind.name(), cacheDir.toString()));
        // log.info(String.format("[%s] Child: %s", eventKind.name(), child.toString()));
        // log.warn(String.format("-- Expected file: %s --", expectedFile));
        // log.warn(String.format("-- Expected cache file: %s --", expectedCacheFile));

        for (String requestedFile : fileCacheStatus.keySet()) {
          String expectedCacheFile = CacheUtil.getLocalPath(requestedFile, conf);

          if (Files.exists(Paths.get(expectedCacheFile))) {
            log.warn(expectedCacheFile + " was found!");
            fileCacheStatus.put(requestedFile, true);
          }

          boolean allFilesCached = true;
          for (boolean cacheStatus : fileCacheStatus.values()) {
            allFilesCached &= cacheStatus;
          }
          if (allFilesCached == true) {
            log.info("All files were cached!");
            return true;
          }
          else {
            log.info("Still missing some files");
          }
        }

        // if (child.toString().equals(expectedFile)) {
        //   log.warn("File was found!");
        //   return true;
        // }

        if (recursive && (eventKind == ENTRY_CREATE)) {
          try {
            if (Files.isDirectory(child, NOFOLLOW_LINKS)) {
              registerAll(child);
            }
          }
          catch (IOException x) {
            // ignore to keep sample readbale
          }
        }
      }

      boolean valid = key.reset();
      if (!valid) {
        watchKeys.remove(key);

        // all directories are inaccessible
        if (watchKeys.isEmpty()) {
          break;
        }
      }
    }
    return false;
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
    WatchKey cacheDirKey = cacheDir.register(watcher, watchEvents, SensitivityWatchEventModifier.HIGH);
    log.info("* Registered " + cacheDir);
    watchKeys.put(cacheDirKey, cacheDir);
  }
}
