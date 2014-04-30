/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.known;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import net.myrrix.batch.JobStep;
import net.myrrix.store.Namespaces;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class CollectKnownItemsStep extends JobStep {

  @Override
  protected Job buildJob() throws IOException {

    String instanceID = getInstanceID();
    long generationID = getGenerationID();

    @SuppressWarnings("unchecked")
    Job collectJob = prepareJob(Namespaces.getTempPrefix(instanceID, generationID) + "userVectors/",
                                Namespaces.getInstanceGenerationPrefix(instanceID, generationID) + "knownItems/",
                                CollectKnownItemsMapper.class,
                                Text.class,
                                Text.class,
                                Reducer.class,
                                Text.class,
                                Text.class);
    if (collectJob == null) {
      return null;
    }
    // Really should read in and exclude tag IDs but doesn't really hurt much
    collectJob.setNumReduceTasks(0);
    setUseTextOutput(collectJob);
    return collectJob;
  }

  public static void main(String[] args) throws Exception {
    run(new CollectKnownItemsStep(), args);
  }

}
