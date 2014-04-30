/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.similar;

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
public final class SimilarReducer
    extends AbstractMyrrixReducer<VarLongWritable,EntityPrefWritable,Text,Text> {

  private int numSimilar;
  private boolean writeJSON;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = getConfiguration();
    numSimilar = Integer.parseInt(conf.get("output.similar.count", "10"));
    String outputRecsFormat = conf.get("output.similar.format", "mahout");
    writeJSON = "json".equals(outputRecsFormat);
    Preconditions.checkArgument(numSimilar > 0, "# similar must be positive: %s", numSimilar);
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
        }), numSimilar);
    context.write(new Text(Long.toString(key.get())), new Text(toSimilarString(recs)));
  }

  private String toSimilarString(Iterable<RecommendedItem> recs) {
    StringBuilder similarString = new StringBuilder(1000);
    similarString.append('[');
    boolean first = true;
    for (RecommendedItem rec : recs) {
      if (first) {
        first = false;
      } else {
        similarString.append(',');
      }
      if (writeJSON) {
        similarString.append('[').append(rec.getItemID()).append(',').append(rec.getValue()).append(']');
      } else {
        similarString.append(rec.getItemID()).append(':').append(rec.getValue());
      }
    }
    similarString.append(']');
    return similarString.toString();
  }

}