/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.publish;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.mahout.math.VarIntWritable;

import net.myrrix.batch.common.AbstractMyrrixMapper;
import net.myrrix.batch.iterate.cluster.WeightedFloatArrayWritable;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class PublishCentroidsMapper
    extends AbstractMyrrixMapper<VarIntWritable,WeightedFloatArrayWritable,Text,Text> {

  @Override
  public void map(VarIntWritable clusterID,
                  WeightedFloatArrayWritable centroid,
                  Context context) throws IOException, InterruptedException {
    context.write(new Text(Integer.toString(clusterID.get())), 
                  new Text(PublishMapper.vectorToString(centroid.getValues())));
  }

}
