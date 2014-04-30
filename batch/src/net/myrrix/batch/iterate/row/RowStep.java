/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.iterate.row;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.math.VarLongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.batch.common.writable.FastByIDFloatMapWritable;
import net.myrrix.batch.common.writable.FloatArrayWritable;
import net.myrrix.batch.common.writable.VarLongRawComparator;
import net.myrrix.batch.iterate.IterationState;
import net.myrrix.batch.iterate.IterationStep;
import net.myrrix.common.random.RandomUtils;
import net.myrrix.store.Namespaces;
import net.myrrix.store.Store;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class RowStep extends IterationStep {

  private static final Logger log = LoggerFactory.getLogger(RowStep.class);

  public static final double DEFAULT_ALPHA = 1.0;
  public static final double DEFAULT_LAMBDA = 0.1;

  public static final String Y_KEY_KEY = "Y_KEY";
  public static final String POPULAR_KEY = "POPULAR";
  public static final String CONVERGENCE_SAMPLING_MODULUS_KEY = "CONVERGENCE_SAMPLING_MODULUS";

  @Override
  protected Job buildJob() throws IOException {

    IterationState iterationState = getIterationState();
    String iterationKey = iterationState.getIterationKey();
    boolean x = iterationState.isComputingX();
    int lastIteration = iterationState.getIteration() - 1;
    Store store = Store.get();

    String instanceID = getInstanceID();
    long generationID = getGenerationID();

    if (store.exists(Namespaces.getInstanceGenerationPrefix(instanceID, generationID) + "X/", false)) {
      // Actually, looks like whole computation of X/Y finished -- just proceed
      return null;
    }

    // Take the opportunity to clean out iteration before last, if computing X
    if (x) {
      String lastLastIterationKey =
          Namespaces.getIterationsPrefix(instanceID, generationID) + (lastIteration - 1) + '/';
      if (store.exists(lastLastIterationKey, false)) {
        log.info("Deleting old iteration data from {}", lastLastIterationKey);
        store.recursiveDelete(lastLastIterationKey);
      }
    }

    String yKey;
    if (x) {
      yKey = Namespaces.getIterationsPrefix(instanceID, generationID) + lastIteration + "/Y/";
    } else {
      yKey = iterationKey + "X/";
    }

    String xKey = iterationKey + (x ? "X/" : "Y/");
    String rKey = Namespaces.getTempPrefix(instanceID, generationID) + (x ? "userVectors/" : "itemVectors/");

    @SuppressWarnings("unchecked")
    Job xJob = prepareJob(rKey,
                          xKey,
                          Mapper.class,
                          VarLongWritable.class,
                          FastByIDFloatMapWritable.class,
                          RowReducer.class,
                          VarLongWritable.class,
                          FloatArrayWritable.class);

    if (xJob == null) {
      return null;
    }

    xJob.setGroupingComparatorClass(VarLongRawComparator.class);
    xJob.setSortComparatorClass(VarLongRawComparator.class);

    Configuration conf = xJob.getConfiguration();
    conf.set(Y_KEY_KEY, yKey);

    String tempKey = Namespaces.getTempPrefix(instanceID, generationID);
    String popularKey = tempKey + (x ? "popularItemsByUserPartition/" : "popularUsersByItemPartition/");
    conf.set(POPULAR_KEY, popularKey);
    
    if (!x) {
      int modulus = chooseConvergenceSamplingModulus(xJob);
      conf.setInt(CONVERGENCE_SAMPLING_MODULUS_KEY, modulus);
      MultipleOutputs.addNamedOutput(xJob, 
                                     "convergence", 
                                     TextOutputFormat.class, 
                                     Text.class, 
                                     Text.class);
    }

    return xJob;
  }

  // Don't weaken to JobContext! for Hadoop 1/2 compatibility
  private static int chooseConvergenceSamplingModulus(Job job) {
    // Kind of arbitrary formula, determined empirically.
    int modulus = RandomUtils.nextTwinPrime(16 * job.getNumReduceTasks() * job.getNumReduceTasks());
    log.info("Using convergence sampling modulus {} to sample about {}% of all user-item pairs for convergence",
             modulus, 100.0f / modulus / modulus);
    return modulus;
  }

  public static void main(String[] args) throws Exception {
    run(new RowStep(), args);
  }

}
