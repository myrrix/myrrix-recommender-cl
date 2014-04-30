/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.online.generation;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.common.parallel.ExecutorUtils;
import net.myrrix.common.LangUtils;
import net.myrrix.common.ReloadingReference;
import net.myrrix.common.collection.FastByIDMap;
import net.myrrix.common.collection.FastIDSet;
import net.myrrix.store.Namespaces;
import net.myrrix.store.Store;

/**
 * @author Sean Owen
 */
final class GenerationLoader {

  private static final Logger log = LoggerFactory.getLogger(GenerationLoader.class);

  private static final Splitter ON_COMMA = Splitter.on(',');
  private static final Pattern COMMA = Pattern.compile(",");

  private final String instanceID;
  private final int partition;
  private final ReloadingReference<List<List<?>>> allPartitions;
  private final FastIDSet recentlyActiveUsers;
  private final FastIDSet recentlyActiveItems;
  private final Object lockForRecent;
  
  GenerationLoader(String instanceID,
                   int partition,
                   ReloadingReference<List<List<?>>> allPartitions,
                   FastIDSet recentlyActiveUsers,
                   FastIDSet recentlyActiveItems,
                   Object lockForRecent) {
    this.instanceID = instanceID;
    this.partition = partition;
    this.allPartitions = allPartitions;
    this.recentlyActiveUsers = recentlyActiveUsers;
    this.recentlyActiveItems = recentlyActiveItems;
    // This must be acquired to access the 'recent' fields above:
    this.lockForRecent = lockForRecent;
  }

  void loadModel(long generationID, Generation currentGeneration) throws IOException {
    String generationPrefix = Namespaces.getInstanceGenerationPrefix(instanceID, generationID);
    log.info("Loading model from {}", generationPrefix);

    Collection<Future<?>> futures = Lists.newArrayList();
    // Limit this fairly sharply to 2 so as to not saturate the network link
    ExecutorService executor = Executors.newFixedThreadPool(
        2,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("LoadModel-%d").build());

    FastIDSet loadedUserIDs;
    FastIDSet loadedItemIDs;
    FastIDSet loadedUserIDsForKnownItems;
    FastIDSet loadedItemTagIDs;
    FastIDSet loadedUserTagIDs;
    try {
      loadedItemTagIDs = loadTagIDs(generationPrefix, true, currentGeneration, futures, executor);
      loadedUserTagIDs = loadTagIDs(generationPrefix, false, currentGeneration, futures, executor);
      loadedUserIDs = loadXOrY(generationPrefix, true, currentGeneration, futures, executor);
      loadedItemIDs = loadXOrY(generationPrefix, false, currentGeneration, futures, executor);

      if (currentGeneration.getKnownItemIDs() == null) {
        loadedUserIDsForKnownItems = null;
      } else {
        loadedUserIDsForKnownItems =
            loadKnownItemIDs(Namespaces.getInstanceGenerationPrefix(instanceID, generationID) + "knownItems/",
                             currentGeneration,
                             futures,
                             executor);
      }

      loadClusters(generationPrefix, true, currentGeneration, futures, executor);
      loadClusters(generationPrefix, false, currentGeneration, futures, executor);

      for (Future<?> future : futures) {
        try {
          future.get();
        } catch (InterruptedException e) {
          throw new IOException(e);
        } catch (ExecutionException e) {
          throw new IOException(e.getCause());
        }
      }
      log.info("Finished all load tasks");
      
    } finally {
      ExecutorUtils.shutdownNowAndAwait(executor);
    }

    synchronized (lockForRecent) {
      // Not recommended to set this to 'false' -- may be useful in rare cases
      if (Boolean.parseBoolean(System.getProperty("model.removeNotUpdatedData", "true"))) {
        log.info("Pruning old entries...");
        removeNotUpdated(currentGeneration.getX().keySetIterator(),
                         loadedUserIDs,
                         recentlyActiveUsers,
                         currentGeneration.getXLock().writeLock());
        removeNotUpdated(currentGeneration.getY().keySetIterator(),
                         loadedItemIDs,
                         recentlyActiveItems,
                         currentGeneration.getYLock().writeLock());
        if (loadedUserIDsForKnownItems != null && currentGeneration.getKnownItemIDs() != null) {
          removeNotUpdated(currentGeneration.getKnownItemIDs().keySetIterator(),
                           loadedUserIDsForKnownItems,
                           recentlyActiveUsers,
                           currentGeneration.getKnownItemLock().writeLock());
        }
        removeNotUpdated(currentGeneration.getItemTagIDs().iterator(),
                         loadedItemTagIDs,
                         recentlyActiveUsers,
                         currentGeneration.getXLock().writeLock());
        removeNotUpdated(currentGeneration.getUserTagIDs().iterator(),
                         loadedUserTagIDs,
                         recentlyActiveItems,
                         currentGeneration.getYLock().writeLock());
      }
      this.recentlyActiveItems.clear();
      this.recentlyActiveUsers.clear();
    }

    log.info("Recomputing generation state...");
    currentGeneration.recomputeState();

    log.info("All model elements loaded, {} users and {} items", 
             currentGeneration.getNumUsers(), currentGeneration.getNumItems());
  }
  
  private static FastIDSet loadTagIDs(String generationPrefix,
                                      boolean isItemTags,
                                      Generation generation,
                                      Collection<Future<?>> futures,
                                      ExecutorService executor) throws IOException {
    String xOrYTagPrefix = generationPrefix + (isItemTags ? 'X' : 'Y') + "Tags/";
    final FastIDSet loadedIDs = new FastIDSet(1000);
    
    final Lock writeLock = isItemTags ? generation.getXLock().writeLock() : generation.getYLock().writeLock();
    final FastIDSet itemOrUserTagIDs = isItemTags ? generation.getItemTagIDs() : generation.getUserTagIDs();

    Iterable<String> xOrYTagFilePrefix = Store.get().list(xOrYTagPrefix, true);
    
    for (final String tagFilePrefix : xOrYTagFilePrefix) {
      futures.add(executor.submit(new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          BufferedReader reader = Store.get().streamFrom(tagFilePrefix);
          try {
            String line;
            while ((line = reader.readLine()) != null) {
              long tagID = Long.parseLong(line);
              writeLock.lock();
              try {
                itemOrUserTagIDs.add(tagID);
                loadedIDs.add(tagID);
              } finally {
                writeLock.unlock();
              }
            }
          } finally {
            reader.close();
          }
          log.info("Loaded tag IDs from {}", tagFilePrefix);          
          return null;
        }
      }));
    }
    return loadedIDs;
  }

  private FastIDSet loadXOrY(String generationPrefix,
                             boolean isX,
                             Generation generation,
                             Collection<Future<?>> futures,
                             ExecutorService executor) throws IOException {

    final int theNumPartitions = allPartitions == null ? 1 : allPartitions.get().size();
    final int thePartition = partition;
    final boolean isPartitioning = isX && theNumPartitions > 1;

    String xOrYPrefix = generationPrefix + (isX ? 'X' : 'Y') + '/';
    final FastIDSet loadedIDs = new FastIDSet(10000);
    
    final Lock writeLock = isX ? generation.getXLock().writeLock() : generation.getYLock().writeLock();
    final FastByIDMap<float[]> xOrYMatrix = isX ? generation.getX() : generation.getY();

    Iterable<String> xOrYFilePrefixes = Store.get().list(xOrYPrefix, true);

    for (final String xOrYFilePrefix : xOrYFilePrefixes) {
      futures.add(executor.submit(new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          BufferedReader reader = Store.get().streamFrom(xOrYFilePrefix);
          try {
            String line;
            while ((line = reader.readLine()) != null) {

              int tab = line.indexOf('\t');
              Preconditions.checkArgument(tab >= 0, "Bad input line in %s: %s", xOrYFilePrefix, line);
              long id = Long.parseLong(line.substring(0, tab));

              if (isPartitioning) {
                // If partitioned, load only a part of X (users)
                int linePartition = LangUtils.mod(id, theNumPartitions);
                if (linePartition != thePartition) {
                  continue;
                }
              }

              float[] elements = readFeatureVector(line.substring(tab + 1));

              writeLock.lock();
              try {
                xOrYMatrix.put(id, elements);
                loadedIDs.add(id);
              } finally {
                writeLock.unlock();
              }
            }
          } finally {
            reader.close();
          }
          log.info("Loaded feature vectors from {}", xOrYFilePrefix);
          return null;
        }
      }));
    }

    return loadedIDs;
  }

  private static float[] readFeatureVector(CharSequence featureVectorString) {
    String[] elementTokens = COMMA.split(featureVectorString);
    int features = elementTokens.length;
    float[] elements = new float[features];
    for (int i = 0; i < features; i++) {
      elements[i] = LangUtils.parseFloat(elementTokens[i]);
    }
    return elements;
  }

  private static FastIDSet loadKnownItemIDs(String knownItemPrefix,
                                            final Generation generation,
                                            Collection<Future<?>> futures,
                                            ExecutorService executor) throws IOException {
    final FastIDSet loadedIDs = new FastIDSet(10000);
    Iterable<String> knownItemFilePrefixes = Store.get().list(knownItemPrefix, true);
    for (final String knownItemFilePrefix : knownItemFilePrefixes) {
      futures.add(executor.submit(new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          BufferedReader reader = Store.get().streamFrom(knownItemFilePrefix);
          try {
            String line;
            while ((line = reader.readLine()) != null) {
              int tab = line.indexOf('\t');
              Preconditions.checkArgument(tab >= 0, "Bad input line in %s: %s", knownItemFilePrefix, line);
              long userID = Long.parseLong(line.substring(0, tab));
              FastIDSet itemIDs = new FastIDSet();
              for (String valueString : ON_COMMA.split(line.substring(tab + 1))) {
                itemIDs.add(Long.parseLong(valueString));
              }

              Lock writeLock = generation.getKnownItemLock().writeLock();
              FastByIDMap<FastIDSet> knownItems = generation.getKnownItemIDs();
              writeLock.lock();
              try {
                knownItems.put(userID, itemIDs);
                loadedIDs.add(userID);
              } finally {
                writeLock.unlock();
              }
            }
          } finally {
            reader.close();
          }
          log.info("Loaded known items from {}", knownItemFilePrefix);
          return null;
        }
      }));
    }
    return loadedIDs;
  }

  private static void loadClusters(String generationPrefix,
                                   final boolean isX,
                                   final Generation generation,
                                   Collection<Future<?>> futures,
                                   ExecutorService executor) {

    final String userOrItem = isX ? "user" : "item";
    final String clusterPrefix = generationPrefix + userOrItem + "Clusters/";
    final String centroidPrefix = generationPrefix + userOrItem + "Centroids/";

    // Do this in one call only
    futures.add(executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {

        Store store = Store.get();
        Map<Integer,float[]> centroidsMap = Maps.newHashMap();

        Iterable<String> centroidFilePrefixes = store.list(centroidPrefix, true);
        for (String centroidFilePrefix : centroidFilePrefixes) {
          BufferedReader reader = store.streamFrom(centroidFilePrefix);
          try {
            String line;
            while ((line = reader.readLine()) != null) {
              int tab = line.indexOf('\t');
              Preconditions.checkArgument(tab >= 0, "Bad input line in %s: %s", centroidFilePrefix, line);
              Integer clusterID = Integer.valueOf(line.substring(0, tab));
              float[] elements = readFeatureVector(line.substring(tab + 1));
              centroidsMap.put(clusterID, elements);
            }
          } finally {
            reader.close();
          }
          log.info("Loaded centroids from {}", centroidFilePrefix);          
        }

        Map<Integer,FastIDSet> clustersMap = Maps.newHashMap();

        Iterable<String> clusterFilePrefixes = store.list(clusterPrefix, true);
        for (String clusterFilePrefix : clusterFilePrefixes) {
          BufferedReader reader = store.streamFrom(clusterFilePrefix);
          try {
            String line;
            while ((line = reader.readLine()) != null) {
              int tab = line.indexOf('\t');
              Preconditions.checkArgument(tab >= 0, "Bad input line in %s: %s", clusterFilePrefix, line);
              long id = Long.parseLong(line.substring(0, tab));
              Integer clusterID = Integer.valueOf(line.substring(tab + 1));
              FastIDSet clusterMembers = clustersMap.get(clusterID);
              if (clusterMembers == null) {
                clusterMembers = new FastIDSet();
                clustersMap.put(clusterID, clusterMembers);
              }
              clusterMembers.add(id);
            }
          } finally {
            reader.close();
          }
          log.info("Loaded {} clusters from {}", userOrItem, clusterFilePrefix);                    
        }

        Preconditions.checkState(centroidsMap.size() >= clustersMap.size(),
                                 "Only %s %s centroids, but %s clusters", 
                                 centroidsMap.size(), userOrItem, clustersMap.size());
        
        Collection<IDCluster> newClusters = Lists.newArrayListWithCapacity(centroidsMap.size());
        for (Map.Entry<Integer,float[]> entry : centroidsMap.entrySet()) {
          Integer clusterID = entry.getKey();
          FastIDSet members = clustersMap.get(clusterID);
          if (members == null) {
            // The centroid had no points
            log.warn("Empty {} cluster {}", userOrItem, clusterID);
          } else {
            newClusters.add(new IDCluster(members, entry.getValue()));
          }
        }

        // But check for centroid-less clusters?
        for (Integer clusterID : clustersMap.keySet()) {
          Preconditions.checkState(centroidsMap.containsKey(clusterID),
                                   "Cluster %s of %s has members but no centroid!", clusterID, userOrItem);
        }

        Collection<IDCluster> existingClusters = isX ? generation.getUserClusters() : generation.getItemClusters();
        Lock clusterWriteLock =
            isX ? generation.getUserClustersLock().writeLock() : generation.getItemClustersLock().writeLock();
        clusterWriteLock.lock();
        try {
          existingClusters.clear();
          existingClusters.addAll(newClusters);
        } finally {
          clusterWriteLock.unlock();
        }

        if (!newClusters.isEmpty()) {
          log.info("Loaded {} clusters and centroids from {} and {}", userOrItem, clusterPrefix, centroidPrefix);
        }
        return null;
      }
    }));
  }

  private static void removeNotUpdated(LongPrimitiveIterator it,
                                       FastIDSet updated,
                                       FastIDSet recentlyActive,
                                       Lock writeLock) {
    writeLock.lock();
    try {
      while (it.hasNext()) {
        long id = it.nextLong();
        if (!updated.contains(id) && !recentlyActive.contains(id)) {
          it.remove();
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

}
