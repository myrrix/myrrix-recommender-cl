/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.known;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.math.VarLongWritable;

import net.myrrix.batch.common.AbstractMyrrixMapper;
import net.myrrix.batch.common.writable.FastByIDFloatMapWritable;
import net.myrrix.common.collection.FastByIDFloatMap;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class CollectKnownItemsMapper
    extends AbstractMyrrixMapper<VarLongWritable,FastByIDFloatMapWritable,Text,Text> {

  @Override
  public void map(VarLongWritable userID,
                  FastByIDFloatMapWritable itemPrefs,
                  Context context) throws IOException, InterruptedException {
    context.write(new Text(Long.toString(userID.get())), new Text(setToString(itemPrefs.getMap())));
  }

  private static String setToString(FastByIDFloatMap map) {
    StringBuilder result = new StringBuilder();
    LongPrimitiveIterator it = map.keySetIterator();
    while (it.hasNext()) {
      if (result.length() > 0) {
        result.append(',');
      }
      result.append(it.nextLong());
    }
    return result.toString();
  }

}
