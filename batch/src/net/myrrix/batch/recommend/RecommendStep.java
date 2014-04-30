/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.recommend;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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
public final class RecommendStep extends IterationStep {

  public static final String Y_KEY_KEY = "Y_KEY";
  public static final String YTAGS_PATH_KEY = "YTAGS_PATH";

  @Override
  protected Job buildJob() throws IOException {

    IterationState iterationState = getIterationState();
    String iterationKey = iterationState.getIterationKey();
    String instanceID = getInstanceID();
    long generationID = getGenerationID();
    String tempPrefix = Namespaces.getTempPrefix(instanceID, generationID);
    
    @SuppressWarnings("unchecked")
    Job recommendJob = prepareJob(tempPrefix + "distributeRecommend/",
                                  tempPrefix + "partialRecommend/",
                                  Mapper.class,
                                  VarIntWritable.class,
                                  IDAndKnownItemsAndFloatArrayWritable.class,
                                  RecommendReducer.class,
                                  VarLongWritable.class,
                                  EntityPrefWritable.class);

    if (recommendJob == null) {
      return null;
    }

    Configuration conf = recommendJob.getConfiguration();
    conf.set(Y_KEY_KEY, iterationKey + "Y/");
    conf.set(YTAGS_PATH_KEY,  Namespaces.getInstanceGenerationPrefix(instanceID, generationID) + "YTags/");

    recommendJob.setPartitionerClass(CopyToAllPartitioner.class);
    recommendJob.setGroupingComparatorClass(VarIntRawComparator.class);
    recommendJob.setSortComparatorClass(VarIntRawComparator.class);

    return recommendJob;
  }

  @Override
  protected String getCustomJobName() {
    return defaultCustomJobName();
  }


  public static void main(String[] args) throws Exception {
    run(new RecommendStep(), args);
  }

}
