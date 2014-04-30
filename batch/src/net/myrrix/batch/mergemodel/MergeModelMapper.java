/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.mergemodel;

import java.io.IOException;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.util.FastMath;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.VarIntWritable;

import net.myrrix.batch.common.AbstractMyrrixMapper;
import net.myrrix.batch.common.PartFilePathFilter;
import net.myrrix.batch.common.iterator.sequencefile.PathType;
import net.myrrix.batch.common.iterator.sequencefile.SequenceFileDirIterable;
import net.myrrix.batch.common.writable.FloatArrayWritable;
import net.myrrix.common.LangUtils;
import net.myrrix.store.Namespaces;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class MergeModelMapper extends AbstractMyrrixMapper<LongWritable,Text,Text,Text> {

  private static final Pattern COMMA = Pattern.compile(",");

  private Array2DRowRealMatrix YTX;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    Configuration conf = getConfiguration();
    String ytxKey = conf.get(MergeModelStep.YTX_KEY);
    Path ytxPath = Namespaces.toPath(ytxKey);

    int maxRow = -1;
    int numRows = 0;
    int numCols = -1;
    for (Pair<VarIntWritable,FloatArrayWritable> record :
        new SequenceFileDirIterable<VarIntWritable,FloatArrayWritable>(ytxPath,
                                                                       PathType.LIST,
                                                                       PartFilePathFilter.INSTANCE,
                                                                       conf)) {
      maxRow = FastMath.max(maxRow, record.getFirst().get());
      numRows++;
      if (numCols < 0) {
        numCols = record.getSecond().get().length;
      }
    }
    Preconditions.checkState(numRows == maxRow + 1, "Expected %s rows instead of %s", maxRow + 1, numRows);

    YTX = new Array2DRowRealMatrix(numRows, numCols);
    for (Pair<VarIntWritable,FloatArrayWritable> record :
         new SequenceFileDirIterable<VarIntWritable,FloatArrayWritable>(ytxPath,
                                                                        PathType.LIST,
                                                                        PartFilePathFilter.INSTANCE,
                                                                        conf)) {
      int rowNum = record.getFirst().get();
      float[] row = record.getSecond().get();
      for (int colNum = 0; colNum < numCols; colNum++) {
        YTX.setEntry(rowNum, colNum, row[colNum]);
      }
    }

  }

  @Override
  protected void map(LongWritable position, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    int tab = line.indexOf('\t');
    Preconditions.checkArgument(tab > 0, "Line must have tab delimiter");
    String itemIDString = line.substring(0, tab);
    String[] tokens = COMMA.split(line.substring(tab + 1));
    int numFeatures = tokens.length;
    double[] featureVector = new double[numFeatures];
    for (int i = 0; i < numFeatures; i++) {
      featureVector[i] = LangUtils.parseDouble(tokens[i]);
    }

    double[] transformedVector = YTX.operate(featureVector);

    StringBuilder vectorString = new StringBuilder();
    for (double d : transformedVector) {
      if (vectorString.length() > 0) {
        vectorString.append(',');
      }
      vectorString.append((float) d);
    }

    context.write(new Text(itemIDString), new Text(vectorString.toString()));
  }

}
