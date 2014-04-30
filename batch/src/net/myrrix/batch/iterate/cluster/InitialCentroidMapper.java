/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.iterate.cluster;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.util.FastMath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.mahout.math.VarLongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.batch.common.AbstractMyrrixMapper;
import net.myrrix.batch.common.DataUtils;
import net.myrrix.batch.common.writable.FloatArrayWritable;
import net.myrrix.common.LangUtils;
import net.myrrix.common.collection.FastIDSet;
import net.myrrix.common.math.SimpleVectorMath;
import net.myrrix.common.random.RandomManager;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class InitialCentroidMapper
    extends AbstractMyrrixMapper<VarLongWritable,FloatArrayWritable,NullWritable,FloatArrayWritable> {

  private static final Logger log = LoggerFactory.getLogger(InitialCentroidMapper.class);
  
  private static final int SELECTION_FUDGE_FACTOR = 4;

  private int numCandidatesToChoose;
  private List<float[]> vectors;
  private FastIDSet tagIDs;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = getConfiguration();  
    int k = conf.getInt(ClusterStep.K_KEY, 0);
    Preconditions.checkArgument(k > 0, "k must be positive: %s", k);
    int numSplits = conf.getInt(ClusterStep.NUM_SPLITS_KEY, 0);
    Preconditions.checkArgument(numSplits > 0, "numSplits must be specified: %s", numSplits);
    if (numSplits <= SELECTION_FUDGE_FACTOR) {
      numCandidatesToChoose = k;      
    } else {
      numCandidatesToChoose = (SELECTION_FUDGE_FACTOR * k) / numSplits;
    }
    vectors = Lists.newArrayListWithCapacity(k);
    tagIDs = DataUtils.readKeysFromTextFile(conf.get(ClusterStep.TAGS_PATH_KEY), conf);
  }

  @Override
  protected void map(VarLongWritable id, FloatArrayWritable featureVector, Context context) {
    if (!tagIDs.contains(id.get())) {
      vectors.add(featureVector.get());
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    Iterable<float[]> centroids = initialCentroids(vectors, numCandidatesToChoose);
    NullWritable key = NullWritable.get();
    FloatArrayWritable value = new FloatArrayWritable();
    for (float[] centroid : centroids) {
      value.set(centroid);
      context.write(key, value);
    }
    super.cleanup(context);
  }

  static List<float[]> initialCentroids(List<float[]> vectors, int numCandidatesToChoose) {
    int numVectors = vectors.size();
    if (numVectors == 0) {
      return Collections.emptyList();
    }

    log.info("Choosing {} centroid candidates", numCandidatesToChoose);
    
    List<float[]> centroids = Lists.newArrayListWithCapacity(numCandidatesToChoose);
    List<Double> centroidNorms = Lists.newArrayListWithCapacity(numCandidatesToChoose);
    RandomGenerator random = RandomManager.getRandom();

    float[] firstCentroid = vectors.get(random.nextInt(numVectors));
    centroids.add(firstCentroid);
    centroidNorms.add(SimpleVectorMath.norm(firstCentroid));

    double[] choices = new double[numVectors];
    while (centroids.size() < numCandidatesToChoose) {

      Arrays.fill(choices, 0.0);
      double totalDistSquared = 0.0;
      for (int i = 0; i < choices.length; i++) {
        float[] vector = vectors.get(i);
        double vectorNorm = SimpleVectorMath.norm(vector);
        double smallestDistSquared = Double.POSITIVE_INFINITY;
        int numCentroids = centroids.size();
        for (int j = 0; j < numCentroids; j++) {
          float[] centroid = centroids.get(j);
          double centroidNorm = centroidNorms.get(j);
          // Using Euclidean distance of endpoints between vectors scaled to unit length.
          // Squared distance is really 2 - 2*cos, but won't matter here:
          double distSquared = 1.0 - SimpleVectorMath.dot(vector, centroid) / centroidNorm / vectorNorm;
          if (LangUtils.isFinite(distSquared)) {
            distSquared = FastMath.max(distSquared, 0.0); // Account for rounding error
            if (distSquared < smallestDistSquared) {
              smallestDistSquared = distSquared;
            }
          }
        }
        if (LangUtils.isFinite(smallestDistSquared)) {
          totalDistSquared += smallestDistSquared;
          choices[i] = smallestDistSquared;
        }
      }

      float[] chosenCentroid;
      if (totalDistSquared <= 0.0) {
        log.warn("Could not choose any next centroid based on distance; picking random one");
        chosenCentroid = vectors.get(random.nextInt(numVectors));
      } else {
        double randomChoice = random.nextDouble() * totalDistSquared;
        totalDistSquared = 0.0;
        int choiceIndex = -1;
        for (int i = 0; i < choices.length; i++) {
          totalDistSquared += choices[i];
          if (totalDistSquared >= randomChoice) {
            choiceIndex = i;
            break;
          }
        }
        Preconditions.checkState(choiceIndex != -1,
                                 "Unable to choose next centroid based on distance? %s",
                                 Arrays.toString(choices));
        chosenCentroid = vectors.get(choiceIndex);
      }
      
      centroids.add(chosenCentroid);
      centroidNorms.add(SimpleVectorMath.norm(chosenCentroid));
    }
    return centroids;
  }

}
