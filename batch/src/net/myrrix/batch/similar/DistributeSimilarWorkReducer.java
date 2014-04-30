/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.similar;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;

import net.myrrix.batch.common.AbstractMyrrixReducer;
import net.myrrix.batch.common.DataUtils;
import net.myrrix.batch.recommend.RecommendStep;
import net.myrrix.common.TopN;
import net.myrrix.common.collection.FastByIDMap;
import net.myrrix.common.collection.FastIDSet;
import net.myrrix.store.Namespaces;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class DistributeSimilarWorkReducer extends AbstractMyrrixReducer<VarIntWritable,
                                                                              IDAndFloatArrayWritable,
                                                                              VarLongWritable,
                                                                              EntityPrefWritable> {

  private FastByIDMap<float[]> partialY;
  private int numSimilar;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = getConfiguration();
    String yKey = conf.get(RecommendStep.Y_KEY_KEY);
    Path yPath = Namespaces.toPath(yKey);
    FastIDSet tagIDs = DataUtils.readKeysFromTextFile(conf.get(DistributeSimilarWorkStep.YTAGS_PATH_KEY), conf);
    partialY = DataUtils.loadPartialY(getPartition(), getNumPartitions(), yPath, tagIDs, conf);
    numSimilar = Integer.parseInt(conf.get("output.similar.count", "10"));
    Preconditions.checkArgument(numSimilar > 0, "# similar must be positive: %s", numSimilar);
  }

  @Override
  public void reduce(VarIntWritable key,
                     Iterable<IDAndFloatArrayWritable> values,
                     Context context) throws IOException, InterruptedException {
    Preconditions.checkState(key.get() == getPartition(),
                             "Key must match partition: %s != %s", key.get(), getPartition());
    VarLongWritable outKey = new VarLongWritable();
    for (IDAndFloatArrayWritable value : values) {
      long itemID = value.getID();
      float[] itemFeatures = value.getValues();
      Iterable<RecommendedItem> mostSimilar = TopN.selectTopN(
          new MostSimilarItemIterator(partialY.entrySet().iterator(), itemID, itemFeatures), numSimilar);
      outKey.set(itemID);
      for (RecommendedItem similar : mostSimilar) {
        context.write(outKey, new EntityPrefWritable(similar.getItemID(), similar.getValue()));
      }
    }
  }

}