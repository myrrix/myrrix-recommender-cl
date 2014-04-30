/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.popular;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VarIntWritable;

import net.myrrix.batch.common.writable.FastIDSetWritable;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class PopularCombiner
    extends Reducer<VarIntWritable,FastIDSetWritable,VarIntWritable,FastIDSetWritable> {

  @Override
  public void reduce(VarIntWritable key,
                     Iterable<FastIDSetWritable> values,
                     Context context) throws IOException, InterruptedException {
    context.write(key, new FastIDSetWritable(PopularReducer.merge(values)));
  }

}