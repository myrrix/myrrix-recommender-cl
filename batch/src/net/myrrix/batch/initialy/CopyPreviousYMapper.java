/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.initialy;

import java.io.IOException;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.VarLongWritable;

import net.myrrix.batch.common.AbstractMyrrixMapper;
import net.myrrix.batch.common.writable.FloatArrayWritable;
import net.myrrix.common.LangUtils;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class CopyPreviousYMapper
    extends AbstractMyrrixMapper<LongWritable,Text,VarLongWritable,FloatArrayWritable> {

  private static final Pattern COMMA = Pattern.compile(",");

  @Override
  protected void map(LongWritable position, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    int tab = line.indexOf('\t');
    Preconditions.checkArgument(tab > 0, "Line must have a tab delimiter");
    VarLongWritable itemID = new VarLongWritable(Long.parseLong(line.substring(0, tab)));
    String[] tokens = COMMA.split(line.substring(tab + 1));
    int numFeatures = tokens.length;
    float[] featureVector = new float[numFeatures];
    for (int i = 0; i < numFeatures; i++) {
      featureVector[i] = LangUtils.parseFloat(tokens[i]);
    }
    context.write(itemID, new FloatArrayWritable(featureVector));
  }
  
}
