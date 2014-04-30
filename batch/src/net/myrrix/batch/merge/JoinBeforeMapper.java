/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.merge;

import java.io.IOException;

import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.math.VarLongWritable;

import net.myrrix.batch.common.AbstractMyrrixMapper;
import net.myrrix.batch.common.join.EntityJoinKey;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class JoinBeforeMapper
    extends AbstractMyrrixMapper<VarLongWritable,EntityPrefWritable,EntityJoinKey,EntityPrefWritable> {

  @Override
  public void map(VarLongWritable key, EntityPrefWritable value, Context context)
      throws IOException, InterruptedException {
    context.write(new EntityJoinKey(key.get(), EntityJoinKey.BEFORE), value);
  }

}