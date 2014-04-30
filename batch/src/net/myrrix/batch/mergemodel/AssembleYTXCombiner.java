/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.mergemodel;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VarIntWritable;

import net.myrrix.batch.common.writable.FloatArrayWritable;

/**
 * @author Sean Owen
 * @since 1.0
 * @see AssembleYTXReducer
 */
public final class AssembleYTXCombiner
    extends Reducer<VarIntWritable,FloatArrayWritable,VarIntWritable,FloatArrayWritable> {
  // Don't extend AbstractMyrrixReducer for Combiners

  @Override
  protected void reduce(VarIntWritable row,
                        Iterable<FloatArrayWritable> values,
                        Context context) throws IOException, InterruptedException {
    AssembleYTXReducer.doReduce(row, values, context);
  }

}
