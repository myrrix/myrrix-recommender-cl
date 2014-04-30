/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.iterate.cluster;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VarIntWritable;

/**
 * @author Sean Owen
 * @since 1.0
 * @see AdjustReducer
 */
public final class AdjustCombiner
    extends Reducer<VarIntWritable,WeightedFloatArrayWritable,VarIntWritable,WeightedFloatArrayWritable> {
  // Don't extend AbstractMyrrixReducer for Combiners

  @Override
  protected void reduce(VarIntWritable centroidID,
                        Iterable<WeightedFloatArrayWritable> values,
                        Context context) throws IOException, InterruptedException {
    AdjustReducer.doReduce(centroidID, values, context);
  }

}
