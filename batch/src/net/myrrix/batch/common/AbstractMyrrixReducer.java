/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.common;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.store.MyrrixConfiguration;
import net.myrrix.store.Namespaces;

/**
 * Abstract superclass of all {@link Reducer}s. It manages common configuration.
 *
 * @see AbstractMyrrixMapper
 * @author Sean Owen
 * @since 1.0
 * @param <K1> input key class
 * @param <V1> input value class
 * @param <K2> output key class
 * @param <V2> output value class
 */
public abstract class AbstractMyrrixReducer<K1 extends Writable,
                                            V1 extends Writable,
                                            K2 extends Writable,
                                            V2 extends Writable> extends Reducer<K1,V1,K2,V2> {

  private static final Logger log = LoggerFactory.getLogger(AbstractMyrrixReducer.class);

  private MyrrixConfiguration configuration;
  private int partition;
  private int numPartitions;

  protected final MyrrixConfiguration getConfiguration() {
    return configuration;
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration rawConfiguration = context.getConfiguration();
    log.info("Setup of {} with config {}", this, rawConfiguration);
    this.configuration = MyrrixConfiguration.get(rawConfiguration);

    Namespaces.setGlobalBucket(configuration.getBucketName());

    numPartitions = context.getNumReduceTasks();
    partition = configuration.getInt("mapred.task.partition", -1);
    log.info("Partition index {} ({} total)", partition, numPartitions);
    Preconditions.checkArgument(numPartitions > 0, "# partitions must be positive: %s", numPartitions);
    Preconditions.checkArgument(partition >= 0 && partition < numPartitions,
                                "Partitions must be in [0,# partitions): %s",
                                partition);
  }

  protected final int getPartition() {
    return partition;
  }

  protected final int getNumPartitions() {
    return numPartitions;
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException  {
    log.info("Cleanup of {}", this);
    super.cleanup(context);
  }

  @Override
  public final String toString() {
    return getClass().getSimpleName();
  }

}
