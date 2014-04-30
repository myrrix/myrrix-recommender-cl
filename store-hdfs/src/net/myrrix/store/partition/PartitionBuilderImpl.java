/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.store.partition;

import java.util.List;

import com.google.common.net.HostAndPort;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class PartitionBuilderImpl implements PartitionBuilder {

  /**
   * @throws UnsupportedOperationException
   */
  @Override
  public List<List<HostAndPort>> loadPartitions(int defaultPort, String bucket, String instanceID) {
    throw new UnsupportedOperationException();
  }

}
