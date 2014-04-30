/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.merge.tag;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class MergeNewOldTagsCombiner extends Reducer<LongWritable,NullWritable,LongWritable,NullWritable> {
  // Extends Reducer only as this is a Combiner

  @Override
  protected void reduce(LongWritable key,
                        Iterable<NullWritable> ignored,
                        Context context) throws IOException, InterruptedException {
    context.write(key, NullWritable.get());
  }

}
