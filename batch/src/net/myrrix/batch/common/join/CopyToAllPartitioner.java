/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.common.join;

import com.google.common.base.Preconditions;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.mahout.math.VarIntWritable;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class CopyToAllPartitioner extends Partitioner<VarIntWritable,Object> {

  @Override
  public int getPartition(VarIntWritable key, Object value, int numPartitions) {
    int partition = key.get();
    Preconditions.checkElementIndex(partition, numPartitions);
    return partition;
  }

}
