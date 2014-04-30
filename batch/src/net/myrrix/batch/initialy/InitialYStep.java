/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.initialy;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.mahout.math.VarLongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.batch.JobStep;
import net.myrrix.batch.common.writable.FloatArrayWritable;
import net.myrrix.batch.common.writable.VarLongRawComparator;
import net.myrrix.store.Namespaces;
import net.myrrix.store.Store;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class InitialYStep extends JobStep {

  private static final Logger log = LoggerFactory.getLogger(InitialYStep.class);

  public static final String FEATURES_KEY = "FEATURES";
  public static final int DEFAULT_FEATURES = 50;

  @Override
  protected Job buildJob() throws IOException {

    String instanceID = getInstanceID();
    long generationID = getGenerationID();
    String iterationsPrefix = Namespaces.getIterationsPrefix(instanceID, generationID);
    Store store = Store.get();

    String initialYKey = iterationsPrefix + "0/Y/";
    if (store.exists(iterationsPrefix, false) &&
        (!store.exists(initialYKey, false) || store.exists(initialYKey + "_SUCCESS", true))) {
      // If iterations exist but iterations/0/Y was deleted, or it exists and clearly succeeded, skip
      return null;
    }
    if (store.exists(Namespaces.getInstanceGenerationPrefix(instanceID, generationID) + "X/", false)) {
      // Actually, looks like whole computation of X/Y finished -- just proceed
      return null;
    }

    Job initialYJob = prepareJob(null,
                                 initialYKey,
                                 FlagNewItemsMapper.class,
                                 VarLongWritable.class,
                                 FloatArrayWritable.class,
                                 InitialYReducer.class,
                                 VarLongWritable.class,
                                 FloatArrayWritable.class);
    if (initialYJob == null) {
      return null;
    }

    MultipleInputs.addInputPath(initialYJob,
                                Namespaces.toPath(Namespaces.getTempPrefix(instanceID, generationID) + "itemVectors/"),
                                SequenceFileInputFormat.class,
                                FlagNewItemsMapper.class);

    // Optionally override by reading in last generation's Y as starting value
    long lastGenerationID = getLastGenerationID();
    if (lastGenerationID >= 0) {
      String yPrefix = Namespaces.getInstanceGenerationPrefix(instanceID, lastGenerationID) + "Y/";
      if (store.exists(yPrefix, false)) {
        MultipleInputs.addInputPath(initialYJob,
                                    Namespaces.toPath(yPrefix),
                                    TextInputFormat.class,
                                    CopyPreviousYMapper.class);
      } else {
        log.warn("Previous generation exists, but no Y; this should only happen if the model was not generated " +
                 "due to insufficient rank");
      }
    }

    initialYJob.setCombinerClass(PreviousOrEmptyFeaturesCombiner.class);
    initialYJob.setGroupingComparatorClass(VarLongRawComparator.class);
    initialYJob.setSortComparatorClass(VarLongRawComparator.class);

    String featuresString = System.getProperty("model.features", Integer.toString(DEFAULT_FEATURES));
    log.info("Using {} features", featuresString);
    initialYJob.getConfiguration().setInt(FEATURES_KEY, Integer.parseInt(featuresString));

    return initialYJob;
  }

  public static void main(String[] args) throws Exception {
    run(new InitialYStep(), args);
  }

}
