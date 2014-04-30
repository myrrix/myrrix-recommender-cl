/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.mergemodel;

import java.io.IOException;
import java.util.Iterator;

import com.google.common.base.Preconditions;
import org.apache.mahout.math.VarIntWritable;

import net.myrrix.batch.common.AbstractMyrrixReducer;
import net.myrrix.batch.common.join.EntityJoinKey;
import net.myrrix.batch.common.writable.FloatArrayWritable;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class MultiplyYTXReducer
    extends AbstractMyrrixReducer<EntityJoinKey,FloatArrayWritable,VarIntWritable,FloatArrayWritable> {

  private float[] lastYT;

  @Override
  protected void reduce(EntityJoinKey itemID,
                        Iterable<FloatArrayWritable> values,
                        Context context) throws IOException, InterruptedException {
    Iterator<FloatArrayWritable> valuesIterator = values.iterator();
    if (itemID.getJoinOrder() == EntityJoinKey.BEFORE) {

      // OK to just overwrite, just means there was no pairing for last
      lastYT = valuesIterator.next().get();

    } else {

      if (lastYT == null) {
        return; // OK, just means no pairing
      }

      float[] YT = lastYT;
      lastYT = null;
      float[] X = valuesIterator.next().get();

      int rows = YT.length;
      int columns = X.length;

      float[] out = new float[columns];
      VarIntWritable outKey = new VarIntWritable();
      FloatArrayWritable outValue = new FloatArrayWritable(out);
      for (int r = 0; r < rows; r++) {
        float multiplier = YT[r];
        for (int c = 0; c < columns; c++) {
          out[c] = multiplier * X[c];
        }
        outKey.set(r);
        context.write(outKey, outValue);
      }

    }
    Preconditions.checkState(!valuesIterator.hasNext(), "Unexpected additional values for key %s", itemID);
  }

}
