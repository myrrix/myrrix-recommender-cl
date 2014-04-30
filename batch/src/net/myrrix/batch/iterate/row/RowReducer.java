/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.iterate.row;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.util.FastMath;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Progressable;
import org.apache.mahout.math.VarLongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.batch.common.AbstractMyrrixReducer;
import net.myrrix.batch.common.PartFilePathFilter;
import net.myrrix.batch.common.iterator.sequencefile.PathType;
import net.myrrix.batch.common.iterator.sequencefile.SequenceFileDirIterable;
import net.myrrix.batch.common.iterator.sequencefile.SequenceFileIterable;
import net.myrrix.batch.common.writable.FastByIDFloatMapWritable;
import net.myrrix.batch.common.writable.FloatArrayWritable;
import net.myrrix.common.LangUtils;
import net.myrrix.common.collection.FastByIDFloatMap;
import net.myrrix.common.collection.FastByIDMap;
import net.myrrix.common.collection.FastIDSet;
import net.myrrix.common.math.MatrixUtils;
import net.myrrix.common.math.SimpleVectorMath;
import net.myrrix.store.Namespaces;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class RowReducer
    extends AbstractMyrrixReducer<VarLongWritable,FastByIDFloatMapWritable,VarLongWritable,FloatArrayWritable> {

  private static final Logger log = LoggerFactory.getLogger(RowReducer.class);

  private static final Pattern PART_FILE_NAME_PATTERN = Pattern.compile("part-r-(\\d+)");

  private double alpha;
  private double lambda;
  private boolean reconstructRMatrix;
  private boolean lossIgnoresUnspecified;
  private int convergenceSamplingModulus;
  private MultipleOutputs<?,?> convergenceSampleOutput;
  private FastByIDMap<float[]> Y;
  private RealMatrix YTY;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    Configuration conf = getConfiguration();

    String alphaProperty = conf.get("model.als.alpha");
    alpha = alphaProperty == null ? RowStep.DEFAULT_ALPHA : LangUtils.parseDouble(alphaProperty);

    String lambdaProperty = conf.get("model.als.lambda");
    lambda = alpha * (lambdaProperty == null ? RowStep.DEFAULT_LAMBDA: LangUtils.parseDouble(lambdaProperty));
    
    // This will cause the ALS algorithm to reconstruction the input matrix R, rather than the
    // matrix P = R > 0 . Don't use this unless you understand it!
    reconstructRMatrix = Boolean.parseBoolean(System.getProperty("model.reconstructRMatrix", "false"));
    // Causes the loss function to exclude entries for any input pairs that do not appear in the
    // input and are implicitly 0
    // Likewise, don't touch this for now unless you know what it does.
    lossIgnoresUnspecified = 
        Boolean.parseBoolean(System.getProperty("model.lossIgnoresUnspecified", "false"));

    log.info("alpha = {}, lambda = {}", alpha, lambda);

    convergenceSamplingModulus = conf.getInt(RowStep.CONVERGENCE_SAMPLING_MODULUS_KEY, -1);
    if (convergenceSamplingModulus >= 0) {
      Preconditions.checkArgument(convergenceSamplingModulus >= 0, 
                                  "Not specified: %s", 
                                  RowStep.CONVERGENCE_SAMPLING_MODULUS_KEY);
      log.info("Sampling for convergence where user/item ID == 0 % {}", convergenceSamplingModulus);
      convergenceSampleOutput = new MultipleOutputs<VarLongWritable,FloatArrayWritable>(context);
    }

    Path popularPath = Namespaces.toPath(conf.get(RowStep.POPULAR_KEY));
    Path yPath = Namespaces.toPath(conf.get(RowStep.Y_KEY_KEY));
    
    FastIDSet expectedIDs = readExpectedIDsFromPartition(conf, popularPath, context);

    log.info("Reading X or Y from {}", yPath);

    Y = new FastByIDMap<float[]>(10000);
    YTY = null;
    int dimension = 0;
    long count = 0;
    for (Pair<VarLongWritable,FloatArrayWritable> record :
         new SequenceFileDirIterable<VarLongWritable,FloatArrayWritable>(yPath, 
                                                                         PathType.LIST,
                                                                         PartFilePathFilter.INSTANCE,
                                                                         conf)) {
      long keyID = record.getFirst().get();          
      float[] vector = record.getSecond().get();
      Preconditions.checkNotNull(vector, "Vector was null for %s?", keyID);
      
      if (YTY == null) {
        dimension = vector.length;
        YTY = new Array2DRowRealMatrix(dimension, dimension);
      }
      for (int row = 0; row < dimension; row++) {
        double rowValue = vector[row];
        for (int col = 0; col < dimension; col++) {
          YTY.addToEntry(row, col, rowValue * vector[col]);
        }
      }

      if (expectedIDs == null || expectedIDs.contains(keyID)) {
        Y.put(keyID, vector);
      }
      
      if (++count % 1000 == 0) {
        context.progress();
      }
    }

    if (YTY == null) {
      log.warn("No data in {}; this is only ignorable if no input has ever been added to the system", yPath);
    }
  }

  @Override
  protected void reduce(VarLongWritable userID,
                        Iterable<FastByIDFloatMapWritable> valuesWritable,
                        Context context) throws IOException, InterruptedException {

    Iterator<FastByIDFloatMapWritable> it = valuesWritable.iterator();
    FastByIDFloatMap values = it.next().getMap();
    Preconditions.checkState(!it.hasNext());
    
    // Start computing Wu = (YT*Cu*Y + lambda*I) = (YT*Y + YT*(Cu-I)*Y + lambda*I),
    // by first starting with a copy of YT * Y. Or, a variant on YT * Y, if LOSS_IGNORES_UNSPECIFIED is set
    RealMatrix Wu = 
        lossIgnoresUnspecified ? 
        partialTransposeTimesSelf(Y, YTY.getRowDimension(), values.entrySet()) : 
        YTY.copy();
    
    double[][] WuData = MatrixUtils.accessMatrixDataDirectly(Wu);
    int features = Wu.getRowDimension();
    double[] YTCupu = new double[features];

    for (FastByIDFloatMap.MapEntry entry : values.entrySet()) {

      double xu = entry.getValue();
      long itemID = entry.getKey();
      float[] vector = Y.get(itemID);
      Preconditions.checkNotNull(vector, "No feature vector for %s", itemID);

      // Wu and YTCupu
      if (reconstructRMatrix) {
        for (int row = 0; row < features; row++) {
          YTCupu[row] += xu * vector[row];
        }
      } else {
        double cu = 1.0 + alpha * FastMath.abs(xu);        
        for (int row = 0; row < features; row++) {
          float vectorAtRow = vector[row];
          double rowValue = vectorAtRow * (cu - 1.0);
          double[] WuDataRow = WuData[row];              
          for (int col = 0; col < features; col++) {
            WuDataRow[col] += rowValue * vector[col];
            //Wu.addToEntry(row, col, rowValue * vector[col]);
          }
          if (xu > 0.0) {
            YTCupu[row] += vectorAtRow * cu;
          }
        }
      }

    }

    Preconditions.checkState(!values.isEmpty(), "No values for user {}?", userID.get());

    double lambdaTimesCount = lambda * values.size();
    for (int x = 0; x < features; x++) {
      WuData[x][x] += lambdaTimesCount;
      //Wu.addToEntry(x, x, lambdaTimesCount);
    }

    float[] xu = MatrixUtils.getSolver(Wu).solveDToF(YTCupu);

    FloatArrayWritable userFeatures = new FloatArrayWritable(xu);
    context.write(userID, userFeatures);

    if (convergenceSampleOutput != null && userID.get() % convergenceSamplingModulus == 0) {
      Text userText = new Text(String.valueOf(userID));
      for (FastByIDMap.MapEntry<float[]> entry : Y.entrySet()) {
        long itemID = entry.getKey();
        if (itemID % convergenceSamplingModulus == 0) {
          float estimate = (float) SimpleVectorMath.dot(xu, entry.getValue());
          Text itemEstimateText = new Text(itemID + "\t" + estimate);
          synchronized (convergenceSampleOutput) {
            convergenceSampleOutput.write("convergence", 
                                          userText, 
                                          itemEstimateText, 
                                          "convergence/part");
          }
        }
      }
      //context.getCounter(Counters.CONVERGENCE_SAMPLES).increment(samples);
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException  {
    if (convergenceSampleOutput != null) {
      convergenceSampleOutput.close();
    }
  }

  private FastIDSet readExpectedIDsFromPartition(Configuration conf,
                                                 Path partitionsPath,
                                                 Progressable progressable) throws IOException {
    FileSystem fs = partitionsPath.getFileSystem(conf);
    if (!fs.exists(partitionsPath)) {
      log.info("No IDs in {}, assuming all are allowed", partitionsPath);
      return null;
    }

    Preconditions.checkState(fs.getFileStatus(partitionsPath).isDir(), "Not a directory: %s", partitionsPath);
    FileStatus[] statuses = fs.listStatus(partitionsPath, PartFilePathFilter.INSTANCE);
    Preconditions.checkState(statuses.length == getNumPartitions(),
                             "Number of partitions doesn't match number of ID files (%s vs %s). " +
                             "Was the number of reducers changed?", statuses.length, getNumPartitions());

    // Shuffle order that the many reducers read the many files
    Collections.shuffle(Arrays.asList(statuses));

    for (FileStatus status : statuses) {
      Path partPath = status.getPath();
      Matcher m = PART_FILE_NAME_PATTERN.matcher(partPath.getName());
      Preconditions.checkState(m.find(), "Bad part path file name: {}", partPath.getName());
      int partPartition = Integer.parseInt(m.group(1));
      if (getPartition() == partPartition) {
        return readExpectedIDs(conf, partPath, progressable);
      }
    }
    throw new IllegalStateException("No file found for partition " + getPartition());
  }

  private static FastIDSet readExpectedIDs(Configuration conf, Path path, Progressable progressable) {
    FastIDSet ids = new FastIDSet(10000);
    long count = 0;
    for (Pair<VarLongWritable,?> record : new SequenceFileIterable<VarLongWritable,NullWritable>(path, true, conf)) {
      ids.add(record.getFirst().get());
      if (++count % 10000 == 0) {
        progressable.progress();
      }
    }
    log.info("Read {} IDs from {}", ids.size(), path);
    return ids;
  }

  private static RealMatrix partialTransposeTimesSelf(FastByIDMap<float[]> M, 
                                                      int dimension, 
                                                      Iterable<FastByIDFloatMap.MapEntry> entries) {
    RealMatrix result = new Array2DRowRealMatrix(dimension, dimension);
    for (FastByIDFloatMap.MapEntry entry : entries) {
      float[] vector = M.get(entry.getKey());
      for (int row = 0; row < dimension; row++) {
        float rowValue = vector[row];
        for (int col = 0; col < dimension; col++) {
          result.addToEntry(row, col, rowValue * vector[col]);
        }
      }
    }
    return result;
  }

  //private enum Counters {
  //  CONVERGENCE_SAMPLES,
  //}

}
