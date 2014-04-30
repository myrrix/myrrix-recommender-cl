/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.mergemodel;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.mahout.math.VarIntWritable;

import net.myrrix.batch.JobStep;
import net.myrrix.batch.common.join.EntityJoinKey;
import net.myrrix.batch.common.join.EntityJoinRawComparator;
import net.myrrix.batch.common.writable.FloatArrayWritable;
import net.myrrix.store.Namespaces;
import net.myrrix.store.Store;
import net.myrrix.store.StoreUtils;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class MultiplyYTXStep extends JobStep {

  private static final Splitter ON_DELIMITER = Splitter.on('/');
  public static final String MODEL_MERGE_INSTANCE_PROPERTY = "model.merge.instanceID";

  @Override
  protected Job buildJob() throws IOException {

    String thisInstanceID = getInstanceID();
    long thisGenerationID = getGenerationID();
    // Experimental
    String otherInstanceID = System.getProperty(MODEL_MERGE_INSTANCE_PROPERTY);
    long otherGenerationID = findMostRecentGeneration(otherInstanceID);

    Job multiplyJob = prepareJob(null,
                                 Namespaces.getTempPrefix(thisInstanceID, thisGenerationID) + "partialYTX/",
                                 null,
                                 EntityJoinKey.class,
                                 FloatArrayWritable.class,
                                 MultiplyYTXReducer.class,
                                 VarIntWritable.class,
                                 FloatArrayWritable.class);
    if (multiplyJob == null) {
      return null;
    }

    multiplyJob.setGroupingComparatorClass(EntityJoinRawComparator.class);
    multiplyJob.setSortComparatorClass(EntityJoinRawComparator.class);

    MultipleInputs.addInputPath(multiplyJob,
                                Namespaces.toPath(
                                    Namespaces.getInstanceGenerationPrefix(thisInstanceID, thisGenerationID) + "Y/"),
                                TextInputFormat.class,
                                YTMapper.class);

    MultipleInputs.addInputPath(multiplyJob,
                                Namespaces.toPath(
                                    Namespaces.getInstanceGenerationPrefix(otherInstanceID, otherGenerationID) + "X/"),
                                TextInputFormat.class,
                                XMapper.class);
    multiplyJob.setPartitionerClass(EntityJoinKey.KeyPartitioner.class);

    return multiplyJob;
  }

  public static void main(String[] args) throws Exception {
    run(new MultiplyYTXStep(), args);
  }

  static long findMostRecentGeneration(String otherInstanceID) throws IOException {
    List<String> otherGenerations = StoreUtils.listGenerationsForInstance(otherInstanceID);
    int count = otherGenerations.size();
    
    String otherInstancePrefix = Namespaces.getInstancePrefix(otherInstanceID);    
    Preconditions.checkState(count > 0, "No generations in %s", otherInstancePrefix);

    Store store = Store.get();    
    for (int i = count - 1; i >= 0; i--) {
      long generationID = parseGenerationFromPrefix(otherGenerations.get(i));
      if (store.exists(Namespaces.getGenerationDoneKey(otherInstanceID, generationID), true)) {
        return generationID;
      }
    }
    throw new IllegalStateException("No complete generations in " + otherInstancePrefix);
  }

  private static long parseGenerationFromPrefix(CharSequence prefix) {
    Iterator<String> tokens = ON_DELIMITER.split(prefix).iterator();
    tokens.next();
    return Long.parseLong(tokens.next());
  }

}
