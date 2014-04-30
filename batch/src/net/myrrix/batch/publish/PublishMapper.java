/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.publish;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.mahout.math.VarLongWritable;

import net.myrrix.batch.common.AbstractMyrrixMapper;
import net.myrrix.batch.common.writable.FloatArrayWritable;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class PublishMapper
    extends AbstractMyrrixMapper<VarLongWritable,FloatArrayWritable,Text,Text> {

  @Override
  public void map(VarLongWritable id,
                  FloatArrayWritable value,
                  Context context) throws IOException, InterruptedException {
    float[] vector = value.get();
    context.write(new Text(Long.toString(id.get())), new Text(vectorToString(vector)));
  }

  static String vectorToString(float[] vector) {
    StringBuilder vectorString = new StringBuilder();
    for (float f : vector) {
      if (vectorString.length() > 0) {
        vectorString.append(',');
      }
      vectorString.append(f);
    }
    return vectorString.toString();
  }

}
