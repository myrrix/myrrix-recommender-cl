/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.online.generation;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.mahout.cf.taste.model.IDMigrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.common.OneWayMigrator;
import net.myrrix.common.ReloadingReference;
import net.myrrix.common.collection.FastByIDMap;
import net.myrrix.common.collection.FastIDSet;
import net.myrrix.common.io.IOUtils;
import net.myrrix.common.math.SolverException;
import net.myrrix.store.StoreUtils;
import net.myrrix.store.Namespaces;
import net.myrrix.store.Store;

/**
 * Implementation specialized for the distributed mode architecture, in cooperating
 * with a Computation Layer.
 * 
 * @author Sean Owen
 * @since 1.0
 */
public final class DelegateGenerationManager implements GenerationManager {

  private static final Logger log = LoggerFactory.getLogger(DelegateGenerationManager.class);

  private static final int WRITES_BETWEEN_UPLOAD;
  static {
    WRITES_BETWEEN_UPLOAD =
          Integer.parseInt(System.getProperty("model.distributed.writesBetweenUpload", "10000"));
    Preconditions.checkArgument(WRITES_BETWEEN_UPLOAD > 0,
                                "Bad model.distributed.writesBetweenUpload: %s", WRITES_BETWEEN_UPLOAD);
  }

  private static final long NO_GENERATION = Long.MIN_VALUE;
  private static final Splitter ON_DELIMITER = Splitter.on('/');

  private final String instanceID;
  private final ScheduledExecutorService executorService;
  private long writeGeneration;
  private long modelGeneration;
  private Writer appender;
  private final File appendTempDir;
  private File appenderTempFile;
  private long countdownToUpload;
  private Generation currentGeneration;
  private final FastIDSet recentlyActiveUsers;
  private final FastIDSet recentlyActiveItems;
  private final IDMigrator hasher;    
  private final GenerationLoader loader;
  private final Semaphore refreshSemaphore;

  public DelegateGenerationManager(String bucket,
                                   String instanceID,
                                   File appendTempDir,
                                   int partition,
                                   ReloadingReference<List<List<?>>> allPartitions) throws IOException {

    Preconditions.checkNotNull(bucket, "Bucket is not specified");
    Preconditions.checkNotNull(instanceID, "Instance ID is not specified");

    Namespaces.setGlobalBucket(bucket);

    this.instanceID = instanceID;
    this.appendTempDir = appendTempDir;
    countdownToUpload = WRITES_BETWEEN_UPLOAD;

    writeGeneration = NO_GENERATION;
    modelGeneration = NO_GENERATION;

    recentlyActiveUsers = new FastIDSet();
    recentlyActiveItems = new FastIDSet();
    hasher = new OneWayMigrator();

    loader = new GenerationLoader(instanceID, partition, allPartitions, recentlyActiveUsers, recentlyActiveItems, this);

    executorService = Executors.newScheduledThreadPool(3,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("DistributedGenerationManager-%d").build());
    refreshSemaphore = new Semaphore(1);

    executorService.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        if (!executorService.isShutdown()) {
          try {
            maybeRollAppender();
          } catch (Throwable t) {
            log.warn("Exception while maybe rolling appender", t);
          }
        }
      }
    }, 2, 2, TimeUnit.MINUTES);
    maybeRollAppender();

    executorService.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        if (!executorService.isShutdown()) {
          try {
            log.debug("Periodic check to see if refresh is needed");
            refresh();
          } catch (Throwable t) {
            log.warn("Exception while refreshing", t);
          }
        }
      }
    }, 7, 7, TimeUnit.MINUTES); // Should be mutually prime with delay set above
    refresh();

  }

  @Override
  public String getBucket() {
    return Namespaces.get().getBucket();
  }

  @Override
  public String getInstanceID() {
    return instanceID;
  }

  private long getMostRecentGeneration() throws IOException {
    List<String> recentGenerationPathStrings = StoreUtils.listGenerationsForInstance(instanceID);
    if (recentGenerationPathStrings.isEmpty()) {
      log.warn("No generation found at all at {}; wrong path?", Namespaces.getInstancePrefix(instanceID));
      return -1L;
    }
    CharSequence mostRecentGenerationPathString = recentGenerationPathStrings.get(recentGenerationPathStrings.size() - 1);
    return parseGenerationFromPrefix(mostRecentGenerationPathString);
  }

  private static long parseGenerationFromPrefix(CharSequence prefix) {
    Iterator<String> tokens = ON_DELIMITER.split(prefix).iterator();
    tokens.next();
    return Long.parseLong(tokens.next());
  }

  private synchronized void closeAppender() {
    if (appender != null) {

      try {
        appender.close();
      } catch (IOException ioe) {
        log.warn("Unable to close {} ({}); aborting and deleting file", appenderTempFile, ioe.toString());
        appender = null;
        if (!appenderTempFile.delete()) {
          log.info("Could not delete {}", appenderTempFile);
        }
        appenderTempFile = null;
        return;
      }

      appender = null;

      final File fileToUpload = appenderTempFile;
      appenderTempFile = null;

      boolean writeGenerationExists = writeGeneration >= 0L;
      if (!writeGenerationExists) {
        log.warn("No write generation to upload file to; ignoring it");
      }

      boolean fileToUploadHasData;
      try {
        fileToUploadHasData = !IOUtils.isGZIPFileEmpty(fileToUpload);
      } catch (IOException ioe) {
        log.warn("Unexpected error checking {} for data; deleting", fileToUpload, ioe);
        if (!fileToUpload.delete()) {
          log.info("Could not delete {}", fileToUpload);
        }
        fileToUploadHasData = false;
      }

      if (writeGenerationExists && fileToUploadHasData) {

        final String appendKey = Namespaces.getInboundPrefix(instanceID, writeGeneration) + fileToUpload.getName();

        Callable<?> uploadCallable = new Callable<Void>() {
          @Override
          public Void call() {
            log.info("Uploading {} to {}", fileToUpload, appendKey);
            Store store = Store.get();
            String appendProgressKey = appendKey + ".inprogress";
            try {
              store.touch(appendProgressKey);
              store.upload(appendKey, fileToUpload, false);
              log.info("Uploaded to {}", appendKey);
            } catch (Throwable t) {
              log.warn("Unable to upload {}! Continuing...", fileToUpload);
              log.warn("Exception was:", t);
            } finally {
              try {
                store.delete(appendProgressKey);
              } catch (IOException e) {
                log.warn("Could not delete {}", appendProgressKey, e);
              }
            }
            if (!fileToUpload.delete()) {
              log.info("Could not delete {}", fileToUpload);
            }
            return null;
          }
        };

        if (executorService.isShutdown()) {
          // Occurring during shutdown, so can't handle exceptions or use the executor
          try {
            uploadCallable.call();
          } catch (Exception e) {
            log.warn("Unexpected error while trying to upload file during shutdown", e);
          }
        } else {
          executorService.submit(uploadCallable);
        }

      } else {
        // Just delete right away
        log.info("File appears to have no data, deleting: {}", fileToUpload);
        if (!fileToUpload.delete()) {
          log.info("Could not delete {}", fileToUpload);
        }
      }
    }
  }

  private synchronized void maybeRollAppender() throws IOException {
    long newMostRecentGeneration = getMostRecentGeneration();
    if (newMostRecentGeneration > writeGeneration || countdownToUpload <= 0) {
      countdownToUpload = WRITES_BETWEEN_UPLOAD;
      // Close and write into *current* write generation first -- but only if it exists
      if (writeGeneration >= 0L) {
        closeAppender();
        writeGeneration = newMostRecentGeneration;
      } else {
        writeGeneration = newMostRecentGeneration;
        closeAppender();
      }
      appenderTempFile = File.createTempFile("myrrix-append-", ".csv.gz", appendTempDir);
      // A small buffer is needed here, but GZIPOutputStream already provides a substantial native buffer
      appender = IOUtils.buildGZIPWriter(appenderTempFile);
    }
  }

  @Override
  public synchronized void close() {
    log.info("Shutting down GenerationManager");
    executorService.shutdown(); // Let others complete
    closeAppender();
  }

  @Override
  public synchronized void refresh() {
    try {
      if (appender != null) {
        appender.flush();
      }
    } catch (IOException e) {
      log.warn("Exception while flushing", e);
    }
    
    if (refreshSemaphore.tryAcquire()) {
      executorService.submit(new RefreshCallable());
    } else {
      log.info("Refresh is already in progress");
    }
  }

  @Override
  public synchronized Generation getCurrentGeneration() {
    return currentGeneration;
  }

  @Override
  public void append(long userID, long itemID, float value, boolean bulk) throws IOException {
    StringBuilder line = new StringBuilder(32);
    line.append(userID).append(',').append(itemID).append(',').append(value).append('\n');
    doAppend(line, userID, itemID);
  }
  
  @Override
  public void appendUserTag(long userID, String tag, float value, boolean bulk) throws IOException {
    StringBuilder line = new StringBuilder(32);
    line.append(userID).append(",\"").append(tag).append("\",").append(value).append('\n');
    long itemID = hasher.toLongID(tag);
    doAppend(line, userID, itemID);
  }
  
  @Override
  public void appendItemTag(String tag, long itemID, float value, boolean bulk) throws IOException {
    StringBuilder line = new StringBuilder(32);
    line.append('"').append(tag).append("\",").append(itemID).append(',').append(value).append('\n');
    long userID = hasher.toLongID(tag);
    doAppend(line, userID, itemID); 
  }

  @Override
  public void remove(long userID, long itemID, boolean bulk) throws IOException {
    StringBuilder line = new StringBuilder(32);
    line.append(userID).append(',').append(itemID).append(",\n");
    doAppend(line, userID, itemID);
  }
  
  private synchronized void doAppend(CharSequence line, long userID, long itemID) throws IOException {
    if (appender != null) {
      appender.append(line);
    }
    recentlyActiveUsers.add(userID);
    recentlyActiveItems.add(itemID);
    countdownToUpload--;
  }

  @Override
  public synchronized void bulkDone() throws IOException {
    appender.flush();
    maybeRollAppender();
  }

  private final class RefreshCallable implements Callable<Void> {

    @Override
    public Void call() {
      try {

        maybeRollAppender();

        long mostRecentModelGeneration = getMostRecentModelGeneration();
        if (mostRecentModelGeneration >= 0L) {

          if (mostRecentModelGeneration > modelGeneration) {
            if (modelGeneration == NO_GENERATION) {
              log.info("Most recent generation {} is the first available one", mostRecentModelGeneration);
            } else {
              log.info("Most recent generation {} is newer than current {}",
                       mostRecentModelGeneration, modelGeneration);
            }

            try {

              Generation theCurrentGeneration = currentGeneration;
              if (theCurrentGeneration == null) {
                FastByIDMap<FastIDSet> newKnownItemsIDs =
                    Boolean.valueOf(System.getProperty(Generation.NO_KNOWN_ITEMS_KEY))
                        ? null
                        : new FastByIDMap<FastIDSet>(10000);
                theCurrentGeneration = new Generation(newKnownItemsIDs,
                                                      new FastByIDMap<float[]>(10000),
                                                      new FastByIDMap<float[]>(10000),
                                                      new FastIDSet(1000),
                                                      new FastIDSet(1000));
              }
              
              loader.loadModel(mostRecentModelGeneration, theCurrentGeneration);
              
              int numItems = theCurrentGeneration.getNumItems();
              int numUsers = theCurrentGeneration.getNumUsers();
              if (numUsers == 0 || numItems == 0) {
                log.warn("Model 'loaded' but had no users, or no items? ({}, {}) Ignoring this data", 
                         numUsers, numItems);
              } else {
                modelGeneration = mostRecentModelGeneration;
                currentGeneration = theCurrentGeneration;
              }

            } catch (OutOfMemoryError oome) {
              log.warn("Increase heap size with -Xmx, decrease new generation size with larger " +
                       "-XX:NewRatio value, and/or use -XX:+UseCompressedOops");
              currentGeneration = null;
              throw oome;
            } catch (SolverException ignored) {
              log.warn("Unable to compute a valid generation yet; waiting for more data");
              currentGeneration = null;
            }
          }

        } else {
          log.info("No available generation, nothing to do");
        }
      } catch (Throwable t) {
        log.warn("Unexpected exception while refreshing", t);
      } finally {
        refreshSemaphore.release();
      }
      return null;
    }

    private long getMostRecentModelGeneration() throws IOException {
      List<String> recentGenerationPathStrings = StoreUtils.listGenerationsForInstance(instanceID);
      Store store = Store.get();
      for (int i = recentGenerationPathStrings.size() - 1; i >= 0; i--) {
        CharSequence recentGenerationPathString = recentGenerationPathStrings.get(i);
        long generationID = parseGenerationFromPrefix(recentGenerationPathString);
        String instanceGenerationPrefix = Namespaces.getInstanceGenerationPrefix(instanceID, generationID);
        if (store.exists(Namespaces.getGenerationDoneKey(instanceID, generationID), true) &&
            store.exists(instanceGenerationPrefix + "X/", false) &&
            store.exists(instanceGenerationPrefix + "Y/", false)) {
          return generationID;
        }
      }
      return -1L;
    }

  }

}
