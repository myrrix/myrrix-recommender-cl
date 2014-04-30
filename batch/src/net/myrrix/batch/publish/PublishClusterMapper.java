/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.publish;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.VarLongWritable;

import net.myrrix.batch.common.AbstractMyrrixMapper;
import net.myrrix.batch.common.DataUtils;
import net.myrrix.batch.common.writable.FloatArrayWritable;
import net.myrrix.batch.iterate.cluster.AdjustMapper;
import net.myrrix.common.collection.FastIDSet;
import net.myrrix.common.math.SimpleVectorMath;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class PublishClusterMapper
    extends AbstractMyrrixMapper<VarLongWritable,FloatArrayWritable,Text,Text> {

  private Map<Integer,float[]> centroids;
  private Map<Integer,Double> centroidNorms;
  private FastIDSet tagIDs;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = getConfiguration();
    centroids = AdjustMapper.loadCentroids(conf);
    centroidNorms = Maps.newHashMapWithExpectedSize(centroids.size());
    for (Map.Entry<Integer,float[]> centroid : centroids.entrySet()) {
      centroidNorms.put(centroid.getKey(), SimpleVectorMath.norm(centroid.getValue()));
    }
    tagIDs = DataUtils.readKeysFromTextFile(conf.get(PublishClusterStep.TAGS_PATH_KEY), conf);      
  }

  @Override
  public void map(VarLongWritable id,
                  FloatArrayWritable value,
                  Context context) throws IOException, InterruptedException {
    long memberID = id.get();
    if (!tagIDs.contains(memberID)) {
      int closestCentroidClusterID = AdjustMapper.findClosestCentroid(value.get(), centroids, centroidNorms);
      if (closestCentroidClusterID >= 0) {
        context.write(new Text(Long.toString(memberID)), new Text(Integer.toString(closestCentroidClusterID)));
      }
    }
  }

}
