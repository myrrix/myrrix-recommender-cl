/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.iterate.cluster;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.mahout.math.VarIntWritable;

import net.myrrix.batch.common.writable.FloatArrayWritable;
import net.myrrix.batch.common.writable.VarIntRawComparator;
import net.myrrix.batch.iterate.IterationState;
import net.myrrix.batch.iterate.IterationStep;
import net.myrrix.store.Namespaces;
import net.myrrix.store.Store;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class ClusterStep extends IterationStep {

  static final int DEFAULT_K = 100;
  static final String K_KEY = "K";
  static final String NUM_SPLITS_KEY = "NUM_SPLITS";  
  public static final String CENTROIDS_PATH_KEY = "CENTROIDS_PATH";
  public static final String TAGS_PATH_KEY = "TAGS_PATH";

  @Override
  protected Job buildJob() throws IOException {

    IterationState iterationState = getIterationState();
    int iteration = iterationState.getIteration();
    if (iteration <= 1) {
      // Don't run on first iteration, since feature vectors would be very random if starting from scratch
      return null;
    }

    boolean x = iterationState.isComputingX();

    String instanceID = getInstanceID();
    long generationID = getGenerationID();

    String finalClusterSuffix = x ? "userClusters/" : "itemClusters/";
    String finalCentroidSuffix = x ? "userCentroids/" : "itemCentroids/";

    if (Store.get().exists(
        Namespaces.getInstanceGenerationPrefix(instanceID, generationID) + finalClusterSuffix, false)) {
      // Actually, looks like whole computation of cluster finished -- just proceed
      return null;
    }

    String iterationKey = iterationState.getIterationKey();
    String centroidKey = iterationKey + finalCentroidSuffix;
    String vectorSourceKey = iterationKey + (x ? "X/" : "Y/");

    int lastIteration = iteration - 1;

    if (lastIteration == 1) {
      // Must generate initial centroids

      Job initialCentroidJob = prepareJob(vectorSourceKey,
                                          centroidKey,
                                          InitialCentroidMapper.class,
                                          NullWritable.class,
                                          FloatArrayWritable.class,
                                          InitialCentroidReducer.class,
                                          VarIntWritable.class,
                                          WeightedFloatArrayWritable.class);
      if (initialCentroidJob == null) {
        return null;
      }

      FileInputFormat.setMaxInputSplitSize(initialCentroidJob, 1L << 25); // ~33MB
      
      Configuration conf = initialCentroidJob.getConfiguration();
      String kSysProperty = "model.cluster." + (x ? "user" : "item") + ".k";
      int k = Integer.parseInt(System.getProperty(kSysProperty, Integer.toString(DEFAULT_K)));
      conf.setInt(K_KEY, k);
      
      int numSplits = new SequenceFileInputFormat().getSplits(initialCentroidJob).size();
      conf.setInt(NUM_SPLITS_KEY, numSplits);
      
      conf.set(TAGS_PATH_KEY, 
               Namespaces.getInstanceGenerationPrefix(instanceID, generationID) + (x ? "XTags/" : "YTags/"));

      initialCentroidJob.setNumReduceTasks(1);
      return initialCentroidJob;
    }

    Job assignJob = prepareJob(vectorSourceKey,
                               centroidKey,
                               AdjustMapper.class,
                               VarIntWritable.class,
                               WeightedFloatArrayWritable.class,
                               AdjustReducer.class,
                               VarIntWritable.class,
                               WeightedFloatArrayWritable.class);
    if (assignJob == null) {
      return null;
    }

    String lastCentroidsKey =
        Namespaces.getIterationsPrefix(instanceID, generationID) + lastIteration + '/' + finalCentroidSuffix;
    Configuration conf = assignJob.getConfiguration();
    conf.set(CENTROIDS_PATH_KEY, lastCentroidsKey);
    
    conf.set(TAGS_PATH_KEY, 
             Namespaces.getInstanceGenerationPrefix(instanceID, generationID) + (x ? "XTags/" : "YTags/"));    

    assignJob.setCombinerClass(AdjustCombiner.class);
    assignJob.setGroupingComparatorClass(VarIntRawComparator.class);
    assignJob.setSortComparatorClass(VarIntRawComparator.class);

    return assignJob;
  }

  public static void main(String[] args) throws Exception {
    run(new ClusterStep(), args);
  }

}
