/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.publish;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import net.myrrix.batch.iterate.IterationState;
import net.myrrix.batch.iterate.IterationStep;
import net.myrrix.batch.iterate.cluster.ClusterStep;
import net.myrrix.store.Namespaces;

/**
 * @author Sean Owen
 */
abstract class PublishClusterStep extends IterationStep {
  
  static final String TAGS_PATH_KEY = "TAGS_PATH";

  @Override
  protected Job buildJob() throws IOException {

    IterationState iterationState = getIterationState();
    String iterationKey = iterationState.getIterationKey();
    boolean x = isX();

    String instanceID = getInstanceID();
    long generationID = getGenerationID();
    String generationPrefix = Namespaces.getInstanceGenerationPrefix(instanceID, generationID);

    @SuppressWarnings("unchecked")    
    Job publishJob = prepareJob(iterationKey + (x ? "X/" : "Y/"),
                                generationPrefix + (x ? "userClusters/" : "itemClusters/"),
                                PublishClusterMapper.class,
                                Text.class,
                                Text.class,
                                Reducer.class,
                                Text.class,
                                Text.class);

    if (publishJob == null) {
      return null;
    }

    Configuration conf = publishJob.getConfiguration();
    conf.set(ClusterStep.CENTROIDS_PATH_KEY, iterationKey + (x ? "userCentroids/" : "itemCentroids/"));
    conf.set(TAGS_PATH_KEY, generationPrefix + (x ? "XTags/" : "YTags/"));

    setUseTextOutput(publishJob);
    publishJob.setNumReduceTasks(0);
    return publishJob;
  }

  abstract boolean isX();

  @Override
  protected final String getCustomJobName() {
    return defaultCustomJobName();
  }

}
