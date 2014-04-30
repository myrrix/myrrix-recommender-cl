/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.mergemodel;

import java.io.IOException;
import java.util.Iterator;

import org.apache.mahout.math.VarIntWritable;

import net.myrrix.batch.common.AbstractMyrrixReducer;
import net.myrrix.batch.common.writable.FloatArrayWritable;

/**
 * @author Sean Owen
 * @since 1.0
 * @see AssembleYTXCombiner
 */
public final class AssembleYTXReducer
    extends AbstractMyrrixReducer<VarIntWritable,FloatArrayWritable,VarIntWritable,FloatArrayWritable> {

  @Override
  protected void reduce(VarIntWritable row,
                        Iterable<FloatArrayWritable> values,
                        Context context) throws IOException, InterruptedException {
    doReduce(row, values, context);
  }

  static void doReduce(VarIntWritable row,
                       Iterable<FloatArrayWritable> values,
                       Context context) throws IOException, InterruptedException {
    Iterator<FloatArrayWritable> valuesIterator = values.iterator();
    float[] sum = valuesIterator.next().get().clone();
    while (valuesIterator.hasNext()) {
      float[] summand = valuesIterator.next().get();
      for (int i = 0; i < sum.length; i++) {
        sum[i] += summand[i];
      }
    }
    context.write(row, new FloatArrayWritable(sum));
  }

}
