/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.store.MyrrixConfiguration;
import net.myrrix.store.Namespaces;

/**
 * Abstract superclass of all {@link Mapper}s. It manages common configuration.
 *
 * @see AbstractMyrrixReducer
 * @author Sean Owen
 * @since 1.0
 * @param <K1> input key class
 * @param <V1> input value class
 * @param <K2> output key class
 * @param <V2> output value class
 */
public abstract class AbstractMyrrixMapper<K1 extends Writable,
                                           V1 extends Writable,
                                           K2 extends Writable,
                                           V2 extends Writable> extends Mapper<K1,V1,K2,V2> {

  private static final Logger log = LoggerFactory.getLogger(AbstractMyrrixMapper.class);

  private MyrrixConfiguration configuration;

  protected final MyrrixConfiguration getConfiguration() {
    return configuration;
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration rawConfiguration = context.getConfiguration();
    log.info("Setup of {} with config {}", this.getClass().getSimpleName(), rawConfiguration);
    this.configuration = MyrrixConfiguration.get(rawConfiguration);
    Namespaces.setGlobalBucket(configuration.getBucketName());
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    log.info("Cleanup of {}", this.getClass().getSimpleName());
  }

  @Override
  public final String toString() {
    return getClass().getSimpleName();
  }

}
