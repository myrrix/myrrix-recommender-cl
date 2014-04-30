/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch;

import java.util.Arrays;

import com.google.common.base.Preconditions;

/**
 * Parameters that are sent to a {@link JobStep} when it executes against a Hadoop/EMR cluster.
 * 
 * @author Sean Owen
 * @since 1.0
 */
public final class JobStepConfig {
  
  private final String bucket;
  private final String instanceID;
  private final long generationID;
  private final long lastGenerationID;
  private final int iteration;
  private final boolean computingX;
  
  public JobStepConfig(String bucket,
                       String instanceID,
                       long generationID,
                       long lastGenerationID,
                       int iteration,
                       boolean computingX) {
    Preconditions.checkNotNull(bucket);
    Preconditions.checkNotNull(instanceID);
    Preconditions.checkArgument(generationID >= 0L, "Generation must be nonnegative: {}", generationID);
    Preconditions.checkArgument(iteration >= 0, "Iteration must be nonnegative: {}", iteration);
    this.bucket = bucket;
    this.instanceID = instanceID;
    this.generationID = generationID;
    this.lastGenerationID = lastGenerationID;
    this.iteration = iteration;
    this.computingX = computingX;
  }
  
  public String getBucket() {
    return bucket;
  }

  public String getInstanceID() {
    return instanceID;
  }

  public long getGenerationID() {
    return generationID;
  }

  public long getLastGenerationID() {
    return lastGenerationID;
  }

  public int getIteration() {
    return iteration;
  }

  public boolean isComputingX() {
    return computingX;
  }
  
  public String[] toArgsArray() {
    return new String[] {
      bucket,
      instanceID,
      Long.toString(generationID),
      Long.toString(lastGenerationID),
      Integer.toString(iteration),
      Boolean.toString(computingX),
    };
  }
  
  public static JobStepConfig fromArgsArray(String... args) {
    Preconditions.checkNotNull(args);
    Preconditions.checkArgument(args.length >= 6);
    return new JobStepConfig(args[0],
                             args[1],
                             Long.parseLong(args[2]),
                             Long.parseLong(args[3]),
                             Integer.parseInt(args[4]),
                             Boolean.parseBoolean(args[5]));
  }
  
  @Override
  public String toString() {
    return Arrays.toString(toArgsArray());
  }

}
