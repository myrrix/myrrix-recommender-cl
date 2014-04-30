/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.mergemodel;

import java.io.IOException;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import net.myrrix.batch.common.AbstractMyrrixMapper;
import net.myrrix.batch.common.join.EntityJoinKey;
import net.myrrix.batch.common.writable.FloatArrayWritable;
import net.myrrix.common.LangUtils;

/**
 * @author Sean Owen
 * @since 1.0
 */
public abstract class MultiplyYTXMapper
    extends AbstractMyrrixMapper<LongWritable,Text,EntityJoinKey,FloatArrayWritable> {

  private static final Pattern COMMA = Pattern.compile(",");
  
  abstract boolean isYT();

  @Override
  protected final void map(LongWritable position, Text value, Context context)
      throws IOException, InterruptedException {
    Pair<Long,float[]> idFeatures = parseIDFeatures(value);
    context.write(new EntityJoinKey(idFeatures.getFirst(), isYT() ? EntityJoinKey.BEFORE : EntityJoinKey.AFTER),
                  new FloatArrayWritable(idFeatures.getSecond()));
  }
  
  /**
   * <p>Parses a line of the form:</p>
   *
   * <p>{@code id\tfloat,float,float,...}</p>
   */
  private static Pair<Long,float[]> parseIDFeatures(Text value) {
    String line = value.toString();
    int tab = line.indexOf('\t');
    Preconditions.checkArgument(tab > 0, "Line must have a tab delimiter");
    long itemID = Long.parseLong(line.substring(0, tab));
    String[] tokens = COMMA.split(line.substring(tab + 1));
    int numFeatures = tokens.length;
    float[] featureVector = new float[numFeatures];
    for (int i = 0; i < numFeatures; i++) {
      featureVector[i] = LangUtils.parseFloat(tokens[i]);
    }
    return new Pair<Long,float[]>(itemID, featureVector);
  }

}
