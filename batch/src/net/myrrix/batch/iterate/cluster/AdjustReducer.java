/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.iterate.cluster;

import java.io.IOException;
import java.util.Iterator;

import org.apache.mahout.math.VarIntWritable;

import net.myrrix.batch.common.AbstractMyrrixReducer;

/**
 * @author Sean Owen
 * @since 1.0
 * @see AdjustCombiner
 */
public final class AdjustReducer
    extends AbstractMyrrixReducer<VarIntWritable,WeightedFloatArrayWritable,VarIntWritable,WeightedFloatArrayWritable> {

  @Override
  protected void reduce(VarIntWritable centroidID,
                        Iterable<WeightedFloatArrayWritable> values,
                        Context context) throws IOException, InterruptedException {
    doReduce(centroidID, values, context);
  }

  static void doReduce(VarIntWritable centroidID,
                       Iterable<WeightedFloatArrayWritable> values,
                       Context context) throws IOException, InterruptedException {
    Iterator<WeightedFloatArrayWritable> iterator = values.iterator();
    WeightedFloatArrayWritable first = iterator.next();
    if (iterator.hasNext()) {

      int totalWeight = first.getWeight();
      float[] totalVector = first.getValues().clone();
      if (totalWeight > 1) {
        for (int i = 0; i < totalVector.length; i++) {
          totalVector[i] *= totalWeight;
        }
      }

      while (iterator.hasNext()) {
        WeightedFloatArrayWritable nextValue = iterator.next();
        int nextWeight = nextValue.getWeight();
        float[] nextVector = nextValue.getValues();
        for (int i = 0; i < totalVector.length; i++) {
          totalVector[i] += nextWeight * nextVector[i];
        }
        totalWeight += nextWeight;
      }

      context.write(centroidID, new WeightedFloatArrayWritable(totalWeight, totalVector));
    } else {
      context.write(centroidID, first);
    }
  }

}
