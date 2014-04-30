/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.merge.tag;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.batch.JobStep;
import net.myrrix.store.Namespaces;
import net.myrrix.store.Store;

abstract class MergeNewOldTagsStep extends JobStep {
  
  private static final Logger log = LoggerFactory.getLogger(MergeNewOldTagsStep.class);

  abstract boolean isX();

  @Override
  protected final Job buildJob() throws IOException {

    String instanceID = getInstanceID();
    long generationID = getGenerationID();
    long lastGenerationID = getLastGenerationID();
    
    String tempTagsSuffix = "new" + (isX() ? "Item" : "User") + "Tags/";
    String currentTagsPrefix = Namespaces.getTempPrefix(instanceID, generationID) + tempTagsSuffix;

    String tagsSuffix = (isX() ? "X" : "Y") + "Tags/";
    
    Job mergeJob = prepareJob(null,
                              Namespaces.getInstanceGenerationPrefix(instanceID, generationID) + tagsSuffix,
                              null,
                              LongWritable.class,
                              NullWritable.class,
                              MergeNewOldTagsReducer.class,
                              Text.class,
                              NullWritable.class);

    if (mergeJob == null) {
      return null;
    }
    
    Store store = Store.get();
    
    boolean anyInput = false;
    if (store.exists(currentTagsPrefix, false)) {
      anyInput = true;
      MultipleInputs.addInputPath(mergeJob,
                                  Namespaces.toPath(currentTagsPrefix),
                                  SequenceFileInputFormat.class,
                                  Mapper.class);
    } else {
      log.info("No tags generated in this generation at {}", currentTagsPrefix);            
    }

    if (lastGenerationID >= 0) {
      String lastTagsPrefix = Namespaces.getInstanceGenerationPrefix(instanceID, lastGenerationID) + tagsSuffix;
      if (store.exists(lastTagsPrefix, false)) {
        anyInput = true;              
        MultipleInputs.addInputPath(mergeJob,
                                    Namespaces.toPath(lastTagsPrefix),
                                    TextInputFormat.class,
                                    TextTagsToLongMapper.class);
      } else {
        log.info("Apparently no tags in previous generation at {}?", lastTagsPrefix);
      }
    }
    
    if (!anyInput) {
      log.info("No input, skipping tags");
      return null;
    }

    setUseTextOutput(mergeJob);    
    mergeJob.setCombinerClass(MergeNewOldTagsCombiner.class);
    mergeJob.setNumReduceTasks(1);

    return mergeJob;
  }

}
