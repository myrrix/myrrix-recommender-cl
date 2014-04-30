/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.popular;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.io.NullWritable;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;

import net.myrrix.batch.common.AbstractMyrrixReducer;
import net.myrrix.batch.common.writable.FastIDSetWritable;
import net.myrrix.common.collection.FastIDSet;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class PopularReducer
    extends AbstractMyrrixReducer<VarIntWritable,FastIDSetWritable,VarLongWritable,NullWritable> {

  @Override
  public void reduce(VarIntWritable key,
                     Iterable<FastIDSetWritable> values,
                     Context context) throws IOException, InterruptedException {
    Preconditions.checkState(key.get() == getPartition(),
                             "Key must match partition: %s != %s", key.get(), getPartition());
    VarLongWritable outKey = new VarLongWritable();
    NullWritable outValue = NullWritable.get();
    LongPrimitiveIterator it = merge(values).iterator();
    while (it.hasNext()) {
      outKey.set(it.nextLong());
      context.write(outKey, outValue);
    }
  }
  
  static FastIDSet merge(Iterable<FastIDSetWritable> values) {
    FastIDSet uniqueValues = new FastIDSet();
    for (FastIDSetWritable value : values) {
      uniqueValues.addAll(value.getIDs());
    }
    return uniqueValues;
  }

}