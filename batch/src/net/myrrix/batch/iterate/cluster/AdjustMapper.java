/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.iterate.cluster;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.batch.common.AbstractMyrrixMapper;
import net.myrrix.batch.common.DataUtils;
import net.myrrix.batch.common.PartFilePathFilter;
import net.myrrix.batch.common.iterator.sequencefile.PathType;
import net.myrrix.batch.common.iterator.sequencefile.SequenceFileDirIterable;
import net.myrrix.batch.common.writable.FloatArrayWritable;
import net.myrrix.common.LangUtils;
import net.myrrix.common.collection.FastIDSet;
import net.myrrix.common.math.SimpleVectorMath;
import net.myrrix.store.Namespaces;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class AdjustMapper
    extends AbstractMyrrixMapper<VarLongWritable,FloatArrayWritable,VarIntWritable,WeightedFloatArrayWritable> {

  private static final Logger log = LoggerFactory.getLogger(AdjustMapper.class);
  
  private Map<Integer,float[]> centroids;
  private Map<Integer,Double> centroidNorms;
  private FastIDSet tagIDs;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = getConfiguration();
    centroids = loadCentroids(conf);
    centroidNorms = Maps.newHashMapWithExpectedSize(centroids.size());
    for (Map.Entry<Integer,float[]> centroid : centroids.entrySet()) {
      centroidNorms.put(centroid.getKey(), SimpleVectorMath.norm(centroid.getValue()));
    }
    tagIDs = DataUtils.readKeysFromTextFile(conf.get(ClusterStep.TAGS_PATH_KEY), conf);    
  }

  /**
   * Loads centroids from a location set into the given {@link Configuration} at {@link ClusterStep#CENTROIDS_PATH_KEY}.
   * 
   * @param configuration {@link Configuration} holding path to load from at {@link ClusterStep#CENTROIDS_PATH_KEY}
   * @return centroids, as map from ID to centroid's vector as {@code float[]}
   */
  public static Map<Integer,float[]> loadCentroids(Configuration configuration) {
    String centroidPathKey = configuration.get(ClusterStep.CENTROIDS_PATH_KEY);
    Preconditions.checkNotNull(centroidPathKey, "No centroid path");
    Map<Integer,float[]> centroids = Maps.newHashMap();
    for (Pair<VarIntWritable,WeightedFloatArrayWritable> keyValue :
         new SequenceFileDirIterable<VarIntWritable,WeightedFloatArrayWritable>(Namespaces.toPath(centroidPathKey),
                                                                                PathType.LIST,
                                                                                PartFilePathFilter.INSTANCE,
                                                                                configuration)) {
      centroids.put(keyValue.getFirst().get(), keyValue.getSecond().getValues());
    }
    int numCentroids = centroids.size();
    log.info("Loaded {} centroids from {}", numCentroids, centroidPathKey);
    return centroids;
  }

  @Override
  protected void map(VarLongWritable id, FloatArrayWritable value, Context context)
      throws IOException, InterruptedException {
    if (!tagIDs.contains(id.get())) {
      float[] vector = value.get();
      int closestCentroidClusterID = findClosestCentroid(vector, centroids, centroidNorms);
      if (closestCentroidClusterID >= 0) {
        context.write(new VarIntWritable(closestCentroidClusterID), new WeightedFloatArrayWritable(vector));
      }
    }
  }

  /**
   * @param vector vector to match against closest centroid
   * @param centroids centroids indexed by ID
   * @param centroidNorms pre-computed centroid norms indexed by ID
   * @return ID of closest centroid
   */
  public static int findClosestCentroid(float[] vector, 
                                        Map<Integer,float[]> centroids,
                                        Map<Integer,Double> centroidNorms) {
    double vectorNorm = SimpleVectorMath.norm(vector);
    int closestCentroidClusterID = -1;
    double highestDot = Double.NEGATIVE_INFINITY;
    for (Map.Entry<Integer,float[]> centroid : centroids.entrySet()) {
      int clusterID = centroid.getKey();
      double dot = SimpleVectorMath.dot(centroid.getValue(), vector) / centroidNorms.get(clusterID) / vectorNorm;
      if (LangUtils.isFinite(dot) && dot > highestDot) {
        highestDot = dot;
        closestCentroidClusterID = clusterID;
      }
    }
    return closestCentroidClusterID;
  }

}
