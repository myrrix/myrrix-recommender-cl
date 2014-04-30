/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.util.FastMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.common.ClassUtils;
import net.myrrix.common.LangUtils;
import net.myrrix.common.collection.FastByIDFloatMap;
import net.myrrix.common.collection.FastByIDMap;
import net.myrrix.common.stats.DoubleWeightedMean;
import net.myrrix.store.Namespaces;
import net.myrrix.store.Store;

/**
 * Superclass of all jobs. A job is a series of MapReduces, or "steps" for Amazon EMR.
 *
 * @author Sean Owen
 * @since 1.0
 */
public abstract class MyrrixJob implements Callable<Void>, Closeable {

  private static final Logger log = LoggerFactory.getLogger(MyrrixJob.class);
  
  public static final double CONVERGENCE_THRESHOLD;
  public static final int MAX_ITERATIONS;
  static {
    String iterationsConvergenceString = System.getProperty("model.als.iterations.convergenceThreshold", "0.001");
    CONVERGENCE_THRESHOLD = Double.parseDouble(iterationsConvergenceString);
    Preconditions.checkState(CONVERGENCE_THRESHOLD > 0.0 && CONVERGENCE_THRESHOLD < 1.0,
                             "Bad convergence threshold %s", CONVERGENCE_THRESHOLD);
    log.info("Convergence threshold: {}", CONVERGENCE_THRESHOLD);
    String maxIterationsString = System.getProperty("model.iterations.max", "30");    
    MAX_ITERATIONS = Integer.parseInt(maxIterationsString);
    Preconditions.checkState(MAX_ITERATIONS > 0, "Bad max iterations %s", MAX_ITERATIONS);
    log.info("Max iterations: {}", MAX_ITERATIONS);
  }

  private static final Pattern TAB = Pattern.compile("\t");

  private String instanceID;
  private long generationID;
  private long lastGenerationID;
  private int startIteration;
  private List<Collection<Class<? extends JobStep>>> preSchedule;
  private List<Collection<Class<? extends JobStep>>> iterationSchedule;
  private List<Collection<Class<? extends JobStep>>> postSchedule;

  /**
   * @return new instance of {@code net.myrrix.batch.DelegateJob}, which will be appropriate to the
   *  current running environment (e.g. Hadoop, or Amazon EMR)
   */
  public static MyrrixJob get() {
    return ClassUtils.loadInstanceOf("net.myrrix.batch.DelegateJob", MyrrixJob.class);
  }

  @Override
  public abstract Void call() throws InterruptedException, IOException, JobException;

  public abstract MyrrixJobState getState() throws IOException;

  /**
   * @return Myrrix instance this job is working on
   */
  public final String getInstanceID() {
    return instanceID;
  }

  public final void setInstanceID(String instanceID) {
    Preconditions.checkNotNull(instanceID);
    this.instanceID = instanceID;
  }

  /**
   * @return Myrrix generation this job is working on
   */
  public final long getGenerationID() {
    return generationID;
  }

  public final void setGenerationID(long generationID) {
    Preconditions.checkArgument(generationID >= 0L, "Generation ID must be nonnegative: %s", generationID);
    this.generationID = generationID;
  }

  public final long getLastGenerationID() {
    return lastGenerationID;
  }

  public final void setLastGenerationID(long lastGenerationID) {
    Preconditions.checkArgument(lastGenerationID >= -1L,
                                "Last generation must be nonnegative or -1: %s",
                                lastGenerationID);
    this.lastGenerationID = lastGenerationID;
  }

  public final int getStartIteration() {
    return startIteration;
  }

  public final void setStartIteration(int startIteration) {
    this.startIteration = startIteration;
  }

  public final List<Collection<Class<? extends JobStep>>> getPreSchedule() {
    return preSchedule;
  }

  public final void setPreSchedule(List<Collection<Class<? extends JobStep>>> preSchedule) {
    this.preSchedule = preSchedule;
  }

  public final List<Collection<Class<? extends JobStep>>> getIterationSchedule() {
    return iterationSchedule;
  }

  public final void setIterationSchedule(List<Collection<Class<? extends JobStep>>> iterationSchedule) {
    this.iterationSchedule = iterationSchedule;
  }

  public final List<Collection<Class<? extends JobStep>>> getPostSchedule() {
    return postSchedule;
  }

  public final void setPostSchedule(List<Collection<Class<? extends JobStep>>> postSchedule) {
    this.postSchedule = postSchedule;
  }
  
  final boolean checkConvergence(int iterationNumber) throws IOException {
    if (iterationNumber < 2) {
      return false;
    }
    if (MAX_ITERATIONS > 0 && iterationNumber >= MAX_ITERATIONS) {
      log.info("Reached iteration limit");      
      return true;
    }
    
    String iterationsPrefix = Namespaces.getIterationsPrefix(getInstanceID(), getGenerationID());
    
    FastByIDMap<FastByIDFloatMap> previousEstimates = 
        readUserItemEstimates(iterationsPrefix + (iterationNumber-1) + "/Y/convergence/");
    FastByIDMap<FastByIDFloatMap> estimates = 
        readUserItemEstimates(iterationsPrefix + iterationNumber + "/Y/convergence/"); 
    Preconditions.checkState(estimates.size() == previousEstimates.size(), 
                             "Estimates and previous estimates not the same size: %s vs %s",
                             estimates.size(), previousEstimates.size());
    
    DoubleWeightedMean averageAbsoluteEstimateDiff = new DoubleWeightedMean();
    for (FastByIDMap.MapEntry<FastByIDFloatMap> entry : estimates.entrySet()) {
      long userID = entry.getKey();
      FastByIDFloatMap itemEstimates = entry.getValue();
      FastByIDFloatMap previousItemEstimates = previousEstimates.get(userID);
      Preconditions.checkState(itemEstimates.size() == previousItemEstimates.size(),
                               "Number of estaimtes doesn't match previous: {} vs {}",
                               itemEstimates.size(), previousItemEstimates.size());
      for (FastByIDFloatMap.MapEntry entry2 : itemEstimates.entrySet()) {
        long itemID = entry2.getKey();
        float estimate = entry2.getValue();
        float previousEstimate = previousItemEstimates.get(itemID);
        averageAbsoluteEstimateDiff.increment(FastMath.abs(estimate - previousEstimate), FastMath.max(0.0, estimate));
      }
    }
    
    double convergenceValue;
    if (averageAbsoluteEstimateDiff.getN() == 0) {
      // Fake value to cover corner case
      convergenceValue = FastMath.pow(2.0, -(iterationNumber+1));
      log.info("No samples for convergence; using artificial convergence value: {}", convergenceValue);            
    } else {
      convergenceValue = averageAbsoluteEstimateDiff.getResult();
      log.info("Avg absolute difference in estimate vs prior iteration: {}", convergenceValue);      
      if (!LangUtils.isFinite(convergenceValue)) {
        log.warn("Invalid convergence value, aborting iteration! {}", convergenceValue);
        return true;
      }
    }

    if (convergenceValue < CONVERGENCE_THRESHOLD) {
      log.info("Converged");          
      return true;
    }
    return false;
  }
  
  private static FastByIDMap<FastByIDFloatMap> readUserItemEstimates(String convergenceSamplePrefix) 
      throws IOException {
    log.info("Reading estimates from {}", convergenceSamplePrefix);
    FastByIDMap<FastByIDFloatMap> userItemEstimate = new FastByIDMap<FastByIDFloatMap>(1000);
    Store store = Store.get();
    for (String prefix : store.list(convergenceSamplePrefix, true)) {
      BufferedReader reader = store.streamFrom(prefix);
      try {
        CharSequence line;
        while ((line = reader.readLine()) != null) {
          String[] tokens = TAB.split(line);
          long userID = Long.parseLong(tokens[0]);
          long itemID = Long.parseLong(tokens[1]);
          float estimate = Float.parseFloat(tokens[2]);
          FastByIDFloatMap itemEstimate = userItemEstimate.get(userID);
          if (itemEstimate == null) {
            itemEstimate = new FastByIDFloatMap(1000);
            userItemEstimate.put(userID, itemEstimate);
          }
          itemEstimate.put(itemID, estimate);
        }
      } finally {
        reader.close();
      }
    }
    return userItemEstimate;
  }

}
