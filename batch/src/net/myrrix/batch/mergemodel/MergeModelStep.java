/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.mergemodel;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.batch.JobStep;
import net.myrrix.store.Namespaces;
import net.myrrix.store.Store;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class MergeModelStep extends JobStep {
  
  private static final Logger log = LoggerFactory.getLogger(MergeModelStep.class);

  static final String YTX_KEY = "YTX_KEY";

  @Override
  protected Job buildJob() throws IOException {

    String otherInstanceID = System.getProperty(MultiplyYTXStep.MODEL_MERGE_INSTANCE_PROPERTY);
    Preconditions.checkNotNull(otherInstanceID, "No value for %s", MultiplyYTXStep.MODEL_MERGE_INSTANCE_PROPERTY);
    long otherGenerationID = MultiplyYTXStep.findMostRecentGeneration(otherInstanceID);

    @SuppressWarnings("unchecked")
    Job mergeJob = prepareJob(Namespaces.getInstanceGenerationPrefix(otherInstanceID, otherGenerationID) + "Y/",
                              Namespaces.getInstanceGenerationPrefix(getInstanceID(), getGenerationID()) + "mergedY/",
                              MergeModelMapper.class,
                              Text.class,
                              Text.class,
                              Reducer.class,
                              Text.class,
                              Text.class);
    if (mergeJob == null) {
      return null;
    }

    mergeJob.setInputFormatClass(TextInputFormat.class);
    setUseTextOutput(mergeJob);
    mergeJob.setNumReduceTasks(0);

    mergeJob.getConfiguration().set(YTX_KEY,
                                    Namespaces.getTempPrefix(getInstanceID(), getGenerationID()) + "YTX/");

    return mergeJob;
  }
  
  @Override
  protected void postRun() {
    log.info("Copying other instance's user tags to this instance");
    String otherInstanceID = System.getProperty(MultiplyYTXStep.MODEL_MERGE_INSTANCE_PROPERTY);
    Store store = Store.get();
    try {
      long otherGenerationID = MultiplyYTXStep.findMostRecentGeneration(otherInstanceID);
      String thisYTagsPrefix = Namespaces.getInstanceGenerationPrefix(getInstanceID(), getGenerationID()) + "YTags/";
      String otherYTagsPrefix = Namespaces.getInstanceGenerationPrefix(otherInstanceID, otherGenerationID) + "YTags/";
      store.recursiveDelete(thisYTagsPrefix);
      store.mkdir(thisYTagsPrefix);
      for (String tagFilePrefix : store.list(otherYTagsPrefix, true)) {
        int lastPath = tagFilePrefix.lastIndexOf('/');
        String fileName = tagFilePrefix.substring(lastPath + 1);
        store.copy(tagFilePrefix, thisYTagsPrefix + fileName);
      }
    } catch (IOException ioe) {
      log.warn("Can't find other generation; failed to move user tags", ioe);
    }
  }

  public static void main(String[] args) throws Exception {
    run(new MergeModelStep(), args);
  }

}
