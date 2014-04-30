/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.merge.tag;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import net.myrrix.batch.common.AbstractMyrrixReducer;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class MergeNewOldTagsReducer extends
    AbstractMyrrixReducer<LongWritable,NullWritable,Text,NullWritable> {
  
  @Override
  protected void reduce(LongWritable key,
                        Iterable<NullWritable> ignored,
                        Context context) throws IOException, InterruptedException {
    context.write(new Text(Long.toString(key.get())), NullWritable.get());
  }

}
