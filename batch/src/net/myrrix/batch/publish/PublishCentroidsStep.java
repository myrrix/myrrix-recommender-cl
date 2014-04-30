/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.publish;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import net.myrrix.batch.iterate.IterationState;
import net.myrrix.batch.iterate.IterationStep;
import net.myrrix.store.Namespaces;

/**
 * @author Sean Owen
 */
abstract class PublishCentroidsStep extends IterationStep {

  @Override
  protected Job buildJob() throws IOException {

    IterationState iterationState = getIterationState();
    String iterationKey = iterationState.getIterationKey();
    boolean x = isX();

    String instanceID = getInstanceID();
    long generationID = getGenerationID();

    String suffix = x ? "userCentroids/" : "itemCentroids/";
    
    @SuppressWarnings("unchecked")    
    Job publishJob = prepareJob(iterationKey + suffix,
                                Namespaces.getInstanceGenerationPrefix(instanceID, generationID) + suffix,
                                PublishCentroidsMapper.class,
                                Text.class,
                                Text.class,
                                Reducer.class,
                                Text.class,
                                Text.class);

    if (publishJob == null) {
      return null;
    }

    setUseTextOutput(publishJob);
    publishJob.setNumReduceTasks(1);
    return publishJob;
  }

  abstract boolean isX();

  @Override
  protected final String getCustomJobName() {
    return defaultCustomJobName();
  }

}
