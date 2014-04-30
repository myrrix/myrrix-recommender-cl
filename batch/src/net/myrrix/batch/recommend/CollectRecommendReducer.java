/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.recommend;

import java.io.IOException;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.math.VarLongWritable;

import net.myrrix.batch.common.AbstractMyrrixReducer;
import net.myrrix.common.SimpleRecommendedItem;
import net.myrrix.common.TopN;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class CollectRecommendReducer
    extends AbstractMyrrixReducer<VarLongWritable,EntityPrefWritable,Text,Text> {

  private int numRecs;
  private boolean writeJSON;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = getConfiguration();
    numRecs = Integer.parseInt(conf.get("output.recs.count", "10"));
    String outputRecsFormat = conf.get("output.recs.format", "mahout");
    writeJSON = "json".equals(outputRecsFormat);
    Preconditions.checkArgument(numRecs > 0, "# recommendations must be positive: %s", numRecs);
  }

  @Override
  public void reduce(VarLongWritable key,
                     Iterable<EntityPrefWritable> values,
                     Context context) throws IOException, InterruptedException {
    Iterable<RecommendedItem> recs = TopN.selectTopN(Iterators.transform(values.iterator(),
        new Function<EntityPrefWritable,RecommendedItem>() {
          @Override
          public RecommendedItem apply(EntityPrefWritable entityPref) {
            return new SimpleRecommendedItem(entityPref.getID(), entityPref.getPrefValue());
          }
        }), numRecs);
    context.write(new Text(Long.toString(key.get())), new Text(toRecsString(recs)));
  }

  private String toRecsString(Iterable<RecommendedItem> recs) {
    StringBuilder recString = new StringBuilder(1000);
    recString.append('[');
    boolean first = true;
    for (RecommendedItem rec : recs) {
      if (first) {
        first = false;
      } else {
        recString.append(',');
      }
      if (writeJSON) {
        recString.append('[').append(rec.getItemID()).append(',').append(rec.getValue()).append(']');
      } else {
        recString.append(rec.getItemID()).append(':').append(rec.getValue());
      }
    }
    recString.append(']');
    return recString.toString();
  }

}
