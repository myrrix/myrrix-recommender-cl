/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.initialy;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VarLongWritable;

import net.myrrix.batch.common.writable.FloatArrayWritable;

/**
 * Reads dummy 0-length feature vectors, and real feature vectors from previous run. Outputs the
 * real feature vector if exists, or a dummy if not.
 * 
 * @author Sean Owen
 * @since 1.0
 */
public final class PreviousOrEmptyFeaturesCombiner
    extends Reducer<VarLongWritable,FloatArrayWritable,VarLongWritable,FloatArrayWritable> {
    // Extends Reducer only as this is a Combiner

  @Override
  protected void reduce(VarLongWritable itemID,
                        Iterable<FloatArrayWritable> values,
                        Context context) throws IOException, InterruptedException {
    FloatArrayWritable toWrite = null;
    for (FloatArrayWritable value : values) {
      toWrite = value;
      if (toWrite.get().length > 0) {
        break;
      }
    }
    context.write(itemID, toWrite);
  }

}
