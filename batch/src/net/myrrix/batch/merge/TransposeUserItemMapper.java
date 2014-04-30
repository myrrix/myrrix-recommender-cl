/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.merge;

import java.io.IOException;

import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.math.VarLongWritable;

import net.myrrix.batch.common.AbstractMyrrixMapper;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class TransposeUserItemMapper
    extends AbstractMyrrixMapper<VarLongWritable,EntityPrefWritable,VarLongWritable,EntityPrefWritable> {

  @Override
  public void map(VarLongWritable userID,
                  EntityPrefWritable itemIDValue,
                  Context context) throws IOException, InterruptedException {
    context.write(new VarLongWritable(itemIDValue.getID()), 
                  new EntityPrefWritable(userID.get(), itemIDValue.getPrefValue()));
  }

}
