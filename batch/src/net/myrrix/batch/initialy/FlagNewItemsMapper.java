/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.initialy;

import java.io.IOException;

import org.apache.mahout.math.VarLongWritable;

import net.myrrix.batch.common.AbstractMyrrixMapper;
import net.myrrix.batch.common.writable.FastByIDFloatMapWritable;
import net.myrrix.batch.common.writable.FloatArrayWritable;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class FlagNewItemsMapper
    extends AbstractMyrrixMapper<VarLongWritable,FastByIDFloatMapWritable,VarLongWritable,FloatArrayWritable> {

  private static final FloatArrayWritable EMPTY = new FloatArrayWritable(new float[0]);

  @Override
  public void map(VarLongWritable itemID,
                  FastByIDFloatMapWritable unused,
                  Context context) throws IOException, InterruptedException {
    context.write(itemID, EMPTY);
  }

}
