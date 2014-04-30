/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.recommend;

import java.io.IOException;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import net.myrrix.batch.common.AbstractMyrrixMapper;
import net.myrrix.batch.common.DataUtils;
import net.myrrix.batch.common.join.EntityJoinKey;
import net.myrrix.common.collection.FastIDSet;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class KnownItemsMapper
    extends AbstractMyrrixMapper<LongWritable,Text,EntityJoinKey,FeaturesOrKnownIDsWritable> {

  private static final Splitter ON_COMMA = Splitter.on(',');

  private final EntityJoinKey outKey;
  private final FeaturesOrKnownIDsWritable outValue;
  private String separator;
  private FastIDSet tagIDs;  

  public KnownItemsMapper() {
    outKey = new EntityJoinKey();
    outKey.setJoinOrder(EntityJoinKey.AFTER);
    outValue = new FeaturesOrKnownIDsWritable();
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = getConfiguration();
    separator = conf.get("mapred.textoutputformat.separator", "\t");
    tagIDs = DataUtils.readKeysFromTextFile(conf.get(DistributeRecommendWorkStep.XTAGS_PATH_KEY), conf);        
  }

  @Override
  public void map(LongWritable lineNumber,
                  Text lineWritable,
                  Context context) throws IOException, InterruptedException {
    String line = lineWritable.toString();
    int dividedAt = line.indexOf(separator);
    Preconditions.checkArgument(dividedAt >= 0, "Bad line: %s", line);
    long userID = Long.parseLong(line.substring(0, dividedAt));
    if (!tagIDs.contains(userID)) {
      outKey.setKey(userID);
      outValue.setKnownIDs(stringToSet(line.substring(dividedAt + 1)));
      context.write(outKey, outValue);
    }
  }

  private static FastIDSet stringToSet(CharSequence values) {
    FastIDSet result = new FastIDSet();
    for (String valueString : ON_COMMA.split(values)) {
      result.add(Long.parseLong(valueString));
    }
    return result;
  }

}