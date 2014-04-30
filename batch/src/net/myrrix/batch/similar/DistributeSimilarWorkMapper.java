/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.similar;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.batch.common.AbstractMyrrixMapper;
import net.myrrix.batch.common.DataUtils;
import net.myrrix.batch.common.writable.FloatArrayWritable;
import net.myrrix.common.collection.FastIDSet;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class DistributeSimilarWorkMapper
    extends AbstractMyrrixMapper<VarLongWritable,FloatArrayWritable,VarIntWritable,IDAndFloatArrayWritable> {

  private static final Logger log = LoggerFactory.getLogger(DistributeSimilarWorkMapper.class);

  private int numReducers;
  private FastIDSet tagIDs;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = getConfiguration();
    numReducers = context.getNumReduceTasks();
    tagIDs = DataUtils.readKeysFromTextFile(conf.get(DistributeSimilarWorkStep.YTAGS_PATH_KEY), conf);
    log.info("Distributing data to {} reducers", numReducers);
  }

  @Override
  public void map(VarLongWritable key,
                  FloatArrayWritable value,
                  Context context) throws IOException, InterruptedException {
    long itemID = key.get();
    if (!tagIDs.contains(itemID)) {
      float[] itemFeatures = value.get();
      VarIntWritable outKey = new VarIntWritable();
      IDAndFloatArrayWritable outValue = new IDAndFloatArrayWritable(itemID, itemFeatures);
      for (int i = 0; i < numReducers; i++) {
        outKey.set(i);
        context.write(outKey, outValue);
      }
    }
  }

}