/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.popular;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;

import net.myrrix.batch.common.AbstractMyrrixMapper;
import net.myrrix.batch.common.writable.FastByIDFloatMapWritable;
import net.myrrix.batch.common.writable.FastIDSetWritable;
import net.myrrix.common.collection.FastIDSet;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class PopularMapper
    extends AbstractMyrrixMapper<VarLongWritable,FastByIDFloatMapWritable,VarIntWritable,FastIDSetWritable> {

  private int numReducers;
  private final Partitioner<VarLongWritable,?> hasher = new HashPartitioner<VarLongWritable,Object>();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    numReducers = context.getNumReduceTasks();
  }

  @Override
  public void map(VarLongWritable id,
                  FastByIDFloatMapWritable values,
                  Context context) throws IOException, InterruptedException {
    FastIDSet targetIDs = new FastIDSet();
    LongPrimitiveIterator it = values.getMap().keySetIterator();
    while (it.hasNext()) {
      targetIDs.add(it.nextLong());
    }
    // Make sure we use exactly the same hash:
    int partition = hasher.getPartition(id, null, numReducers);
    context.write(new VarIntWritable(partition), new FastIDSetWritable(targetIDs));
  }

}