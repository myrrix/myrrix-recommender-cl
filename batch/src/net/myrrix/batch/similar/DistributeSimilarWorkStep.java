/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.similar;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;

import net.myrrix.batch.common.join.CopyToAllPartitioner;
import net.myrrix.batch.common.writable.VarIntRawComparator;
import net.myrrix.batch.iterate.IterationState;
import net.myrrix.batch.iterate.IterationStep;
import net.myrrix.store.Namespaces;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class DistributeSimilarWorkStep extends IterationStep {

  public static final String Y_KEY_KEY = "Y_KEY";
  public static final String YTAGS_PATH_KEY = "YTAGS_PATH";

  @Override
  protected Job buildJob() throws IOException {

    IterationState iterationState = getIterationState();
    String iterationKey = iterationState.getIterationKey();
    String instanceID = getInstanceID();
    long generationID = getGenerationID();
    String tempPrefix = Namespaces.getTempPrefix(instanceID, generationID);

    String yKey = iterationKey + "Y/";
    Job similarJob = prepareJob(yKey,
                                tempPrefix + "distributeSimilar/",
                                DistributeSimilarWorkMapper.class,
                                VarIntWritable.class,
                                IDAndFloatArrayWritable.class,
                                DistributeSimilarWorkReducer.class,
                                VarLongWritable.class,
                                EntityPrefWritable.class);

    if (similarJob == null) {
      return null;
    }

    Configuration conf = similarJob.getConfiguration();
    conf.set(Y_KEY_KEY, yKey);
    conf.set(YTAGS_PATH_KEY, Namespaces.getInstanceGenerationPrefix(instanceID, generationID) + "YTags/");
    
    similarJob.setPartitionerClass(CopyToAllPartitioner.class);
    similarJob.setGroupingComparatorClass(VarIntRawComparator.class);
    similarJob.setSortComparatorClass(VarIntRawComparator.class);

    return similarJob;
  }

  @Override
  protected String getCustomJobName() {
    return defaultCustomJobName();
  }

  public static void main(String[] args) throws Exception {
    run(new DistributeSimilarWorkStep(), args);
  }

}
