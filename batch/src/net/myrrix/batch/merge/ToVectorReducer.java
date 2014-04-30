/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.merge;

import java.io.IOException;

import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.math.VarLongWritable;

import net.myrrix.batch.common.AbstractMyrrixReducer;
import net.myrrix.batch.common.writable.FastByIDFloatMapWritable;
import net.myrrix.common.collection.FastByIDFloatMap;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class ToVectorReducer
    extends AbstractMyrrixReducer<VarLongWritable,EntityPrefWritable,VarLongWritable,FastByIDFloatMapWritable> {

  @Override
  public void reduce(VarLongWritable id,
                     Iterable<EntityPrefWritable> values,
                     Context context) throws IOException, InterruptedException {
    FastByIDFloatMap map = new FastByIDFloatMap();
    for (EntityPrefWritable value : values) {
      map.put(value.getID(), value.getPrefValue());
    }
    if (!map.isEmpty()) {
      context.write(id, new FastByIDFloatMapWritable(map));
    }
  }

}
