/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.merge.tag;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import net.myrrix.batch.common.AbstractMyrrixMapper;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class TextTagsToLongMapper
    extends AbstractMyrrixMapper<LongWritable,Text,LongWritable,NullWritable> {

  @Override
  public void map(LongWritable line, Text value, Context context)
      throws IOException, InterruptedException {
    context.write(new LongWritable(Long.parseLong(value.toString().trim())), NullWritable.get());
  }

}