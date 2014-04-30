/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.recommend;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.math.VarLongWritable;

import net.myrrix.batch.common.AbstractMyrrixMapper;
import net.myrrix.batch.common.DataUtils;
import net.myrrix.batch.common.join.EntityJoinKey;
import net.myrrix.batch.common.writable.FloatArrayWritable;
import net.myrrix.common.collection.FastIDSet;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class UserFeaturesMapper
    extends AbstractMyrrixMapper<VarLongWritable,FloatArrayWritable,EntityJoinKey,FeaturesOrKnownIDsWritable> {

  private final EntityJoinKey outKey;
  private final FeaturesOrKnownIDsWritable outValue;
  private FastIDSet tagIDs;

  public UserFeaturesMapper() {
    outKey = new EntityJoinKey();
    outKey.setJoinOrder(EntityJoinKey.BEFORE);
    outValue = new FeaturesOrKnownIDsWritable();
  }
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = getConfiguration();
    tagIDs = DataUtils.readKeysFromTextFile(conf.get(DistributeRecommendWorkStep.XTAGS_PATH_KEY), conf);
  }

  @Override
  public void map(VarLongWritable key,
                  FloatArrayWritable features,
                  Context context) throws IOException, InterruptedException {
    long userID = key.get();
    if (!tagIDs.contains(userID)) {
      outKey.setKey(userID);
      outValue.setFeatures(features);
      context.write(outKey, outValue);
    }
  }

}