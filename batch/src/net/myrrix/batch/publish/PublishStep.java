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

abstract class PublishStep extends IterationStep {

  @Override
  protected Job buildJob() throws IOException {

    IterationState iterationState = getIterationState();
    String iterationKey = iterationState.getIterationKey();
    String xOrY = isX() ? "X/" : "Y/";

    @SuppressWarnings("unchecked")    
    Job publishJob = prepareJob(iterationKey + xOrY,
                                Namespaces.getInstanceGenerationPrefix(getInstanceID(), getGenerationID()) + xOrY,
                                PublishMapper.class,
                                Text.class,
                                Text.class,
                                Reducer.class,
                                Text.class,
                                Text.class);

    if (publishJob == null) {
      return null;
    }
    setUseTextOutput(publishJob);
    publishJob.setNumReduceTasks(0);
    return publishJob;
  }

  abstract boolean isX();

  @Override
  protected String getCustomJobName() {
    return defaultCustomJobName();
  }

}
