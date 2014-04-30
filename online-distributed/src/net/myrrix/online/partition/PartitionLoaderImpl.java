/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.online.partition;

import java.util.List;

import com.google.common.net.HostAndPort;

import net.myrrix.common.ClassUtils;
import net.myrrix.store.partition.PartitionBuilder;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class PartitionLoaderImpl implements PartitionLoader {

  /**
   * @return an instance of {@code net.myrrix.store.partition.PartitionBuilderImpl}
   */
  @Override
  public List<List<HostAndPort>> loadPartitions(int defaultPort, String bucket, String instanceID) {
    PartitionBuilder builder =
        ClassUtils.loadInstanceOf("net.myrrix.store.partition.PartitionBuilderImpl", PartitionBuilder.class);
    return builder.loadPartitions(defaultPort, bucket, instanceID);
  }

}
