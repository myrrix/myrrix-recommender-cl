/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.iterate.cluster;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.NullWritable;
import org.apache.mahout.math.VarIntWritable;

import net.myrrix.batch.common.AbstractMyrrixReducer;
import net.myrrix.batch.common.writable.FloatArrayWritable;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class InitialCentroidReducer
    extends AbstractMyrrixReducer<NullWritable,FloatArrayWritable,VarIntWritable,WeightedFloatArrayWritable> {

  private int k;
  private List<float[]> vectors;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    k = getConfiguration().getInt(ClusterStep.K_KEY, 0);
    Preconditions.checkArgument(k > 0, "k must be positive: %s", k);
    vectors = Lists.newArrayListWithCapacity(k);
  }

  @Override
  protected void reduce(NullWritable key, Iterable<FloatArrayWritable> values, Context context) {
    for (FloatArrayWritable value : values) {
      vectors.add(value.get());
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    List<float[]> centroids = InitialCentroidMapper.initialCentroids(vectors, k);
    for (int i = 0; i < centroids.size(); i++) {
      context.write(new VarIntWritable(i), new WeightedFloatArrayWritable(centroids.get(i)));
    }
    super.cleanup(context);
  }


}
