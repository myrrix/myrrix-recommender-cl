/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.recommend;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.mahout.math.VarIntWritable;

import net.myrrix.batch.common.join.EntityJoinKey;
import net.myrrix.batch.common.join.EntityJoinRawComparator;
import net.myrrix.batch.iterate.IterationState;
import net.myrrix.batch.iterate.IterationStep;
import net.myrrix.store.Namespaces;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class DistributeRecommendWorkStep extends IterationStep {

  static final String XTAGS_PATH_KEY = "XTAGS_PATH";
  
  @Override
  protected Job buildJob() throws IOException {

    IterationState iterationState = getIterationState();
    String iterationKey = iterationState.getIterationKey();
    String instanceID = getInstanceID();
    long generationID = getGenerationID();

    Job recommendJob = prepareJob(null,
                                  Namespaces.getTempPrefix(instanceID, generationID) + "distributeRecommend/",
                                  null,
                                  EntityJoinKey.class,
                                  FeaturesOrKnownIDsWritable.class,
                                  DistributeRecommendWorkReducer.class,
                                  VarIntWritable.class,
                                  IDAndKnownItemsAndFloatArrayWritable.class);

    if (recommendJob == null) {
      return null;
    }

    recommendJob.setGroupingComparatorClass(EntityJoinRawComparator.class);
    recommendJob.setSortComparatorClass(EntityJoinRawComparator.class);
    
    Configuration conf = recommendJob.getConfiguration();
    conf.set(XTAGS_PATH_KEY, Namespaces.getInstanceGenerationPrefix(instanceID, generationID) + "XTags/");

    MultipleInputs.addInputPath(recommendJob,
                                Namespaces.toPath(iterationKey + "X/"),
                                SequenceFileInputFormat.class,
                                UserFeaturesMapper.class);

    MultipleInputs.addInputPath(recommendJob,
                                Namespaces.toPath(
                                    Namespaces.getInstanceGenerationPrefix(instanceID, generationID) + "knownItems/"),
                                TextInputFormat.class,
                                KnownItemsMapper.class);

    recommendJob.setPartitionerClass(EntityJoinKey.KeyPartitioner.class);
    return recommendJob;
  }

  @Override
  protected String getCustomJobName() {
    return defaultCustomJobName();
  }


  public static void main(String[] args) throws Exception {
    run(new DistributeRecommendWorkStep(), args);
  }

}
