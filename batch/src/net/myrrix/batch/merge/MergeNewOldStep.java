/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.merge;

import java.io.IOException;
import java.util.Collection;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.math.VarLongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.batch.JobStep;
import net.myrrix.batch.common.join.EntityJoinKey;
import net.myrrix.batch.common.join.EntityJoinRawComparator;
import net.myrrix.common.ClassUtils;
import net.myrrix.store.Namespaces;
import net.myrrix.store.Store;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class MergeNewOldStep extends JobStep {

  private static final Logger log = LoggerFactory.getLogger(MergeNewOldStep.class);

  static final String MODEL_DECAY_FACTOR_KEY = "MODEL_DECAY_FACTOR";
  static final String MODEL_DECAY_ZERO_THRESHOLD = "MODEL_DECAY_ZERO_THRESHOLD";

  @Override
  protected Job buildJob() throws IOException {

    String instanceID = getInstanceID();
    long generationID = getGenerationID();
    long lastGenerationID = getLastGenerationID();

    Job mergeJob = prepareJob(null,
                              Namespaces.getInputPrefix(instanceID, generationID),
                              null,
                              EntityJoinKey.class,
                              EntityPrefWritable.class,
                              MergeNewOldValuesReducer.class,
                              VarLongWritable.class,
                              EntityPrefWritable.class);

    if (mergeJob == null) {
      return null;
    }

    mergeJob.setPartitionerClass(EntityJoinKey.KeyPartitioner.class);
    mergeJob.setGroupingComparatorClass(EntityJoinRawComparator.class);
    mergeJob.setSortComparatorClass(EntityJoinRawComparator.class);

    MultipleInputs.addInputPath(mergeJob,
                                determineInputPath(),
                                determineInputFormat(),
                                DelimitedInputMapper.class);

    if (lastGenerationID >= 0) {
      String inputPrefix = Namespaces.getInputPrefix(instanceID, lastGenerationID);
      Preconditions.checkState(Store.get().exists(inputPrefix, false), "Input path does not exist: %s", inputPrefix);
      MultipleInputs.addInputPath(mergeJob,
                                  Namespaces.toPath(inputPrefix),
                                  SequenceFileInputFormat.class,
                                  JoinBeforeMapper.class);
    }

    MultipleOutputs.addNamedOutput(mergeJob,
                                   "newItemTags",
                                   SequenceFileOutputFormat.class,
                                   LongWritable.class,
                                   NullWritable.class);
    MultipleOutputs.addNamedOutput(mergeJob,
                                   "newUserTags",
                                   SequenceFileOutputFormat.class,
                                   LongWritable.class,
                                   NullWritable.class);

    Configuration conf = mergeJob.getConfiguration();

    String modelDecayString = System.getProperty("model.decay.factor");
    if (modelDecayString != null) {
      conf.setFloat(MODEL_DECAY_FACTOR_KEY, Float.parseFloat(modelDecayString));
    }
    String modelDecayZeroThresholdString = System.getProperty("model.decay.zeroThreshold");
    if (modelDecayZeroThresholdString != null) {
      conf.setFloat(MODEL_DECAY_ZERO_THRESHOLD, Float.parseFloat(modelDecayZeroThresholdString));
    }

    return mergeJob;
  }
  
  @Override
  protected void postRun() {
    try {
      move("newItemTags/");
      move("newUserTags/");
    } catch (IOException ioe) {
      log.warn("Failed to move tags output", ioe);
    }
  }
  
  private void move(String itemOrUserSuffix) throws IOException {
    String instanceID = getInstanceID();
    long generationID = getGenerationID();
    String fromPrefix = Namespaces.getInputPrefix(instanceID, generationID) + itemOrUserSuffix;    
    Store store = Store.get();
    if (store.exists(fromPrefix, false)) {
      String toDirPrefix = Namespaces.getTempPrefix(instanceID, generationID) + itemOrUserSuffix;
      Collection<String> tagFiles = store.list(fromPrefix, true);
      if (tagFiles.isEmpty()) {
        log.info("No tag files to move");
      } else {
        log.info("Moving contents of {} to directory {}", fromPrefix, toDirPrefix);
        store.mkdir(toDirPrefix);
        for (String prefix : tagFiles) {
          int lastPath = prefix.lastIndexOf('/');
          String fileName = prefix.substring(lastPath + 1);
          store.move(prefix, toDirPrefix + fileName);
        }
      }
      store.recursiveDelete(fromPrefix);
    } else {
      log.info("No tag file to move at {}", fromPrefix);
    }
  }
  
  private Path determineInputPath() {
    String customInputPath = System.getProperty("batch.input.customPath");
    if (customInputPath == null) {
      return Namespaces.toPath(Namespaces.getInboundPrefix(getInstanceID(), getGenerationID()));
    }
    return new Path(customInputPath);
  }

  private static Class<? extends InputFormat<?,?>> determineInputFormat() {
    String customInputFormatClass = System.getProperty("batch.input.customFormatClass");
    if (customInputFormatClass == null) {
      return TextInputFormat.class;
    }
    @SuppressWarnings("unchecked")
    Class<? extends InputFormat<?,?>> inputFormatClass = 
        (Class<? extends InputFormat<?,?>>) ClassUtils.loadClass(customInputFormatClass, InputFormat.class);
    return inputFormatClass;
  }

  public static void main(String[] args) throws Exception {
    run(new MergeNewOldStep(), args);
  }

}
