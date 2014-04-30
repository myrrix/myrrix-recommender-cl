/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.recommend;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;

import net.myrrix.batch.common.AbstractMyrrixReducer;
import net.myrrix.batch.common.DataUtils;
import net.myrrix.common.TopN;
import net.myrrix.common.collection.FastByIDMap;
import net.myrrix.common.collection.FastIDSet;
import net.myrrix.store.Namespaces;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class RecommendReducer extends AbstractMyrrixReducer<VarIntWritable,
                                                                  IDAndKnownItemsAndFloatArrayWritable,
                                                                  VarLongWritable,
                                                                  EntityPrefWritable> {

  private int numRecs;
  private FastByIDMap<float[]> partialY;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = getConfiguration();
    String yKey = conf.get(RecommendStep.Y_KEY_KEY);
    Path yPath = Namespaces.toPath(yKey);
    FastIDSet tagIDs = DataUtils.readKeysFromTextFile(conf.get(RecommendStep.YTAGS_PATH_KEY), conf);    
    partialY = DataUtils.loadPartialY(getPartition(), getNumPartitions(), yPath, tagIDs, conf);
    numRecs = Integer.parseInt(conf.get("output.recs.count", "10"));
    Preconditions.checkArgument(numRecs > 0, "# recs must be positive: %s", numRecs);
  }

  @Override
  public void reduce(VarIntWritable key,
                     Iterable<IDAndKnownItemsAndFloatArrayWritable> values,
                     Context context) throws IOException, InterruptedException {
    Preconditions.checkState(key.get() == getPartition(),
                             "Key must match partition: %s != %s", key.get(), getPartition());
    for (IDAndKnownItemsAndFloatArrayWritable value : values) {
      long userID = value.getID();
      float[] userFeatures = value.getValues();
      FastIDSet knownItemIDs = value.getKnownItems();
      Iterable<RecommendedItem> recs = TopN.selectTopN(
          new RecommendIterator(userFeatures, partialY.entrySet().iterator(), knownItemIDs),
          numRecs);
      VarLongWritable outKey = new VarLongWritable(userID);
      for (RecommendedItem rec : recs) {
        context.write(outKey, new EntityPrefWritable(rec.getItemID(), rec.getValue()));
      }
    }

  }

}