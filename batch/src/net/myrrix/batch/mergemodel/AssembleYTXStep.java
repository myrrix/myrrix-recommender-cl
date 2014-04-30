/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.mergemodel;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VarIntWritable;

import net.myrrix.batch.JobStep;
import net.myrrix.batch.common.writable.FloatArrayWritable;
import net.myrrix.batch.common.writable.VarIntRawComparator;
import net.myrrix.store.Namespaces;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class AssembleYTXStep extends JobStep {

  @Override
  protected Job buildJob() throws IOException {
    String tempKey = Namespaces.getTempPrefix(getInstanceID(), getGenerationID());
    @SuppressWarnings("unchecked")
    Job assembleJob = prepareJob(tempKey + "partialYTX/",
                                 tempKey + "YTX/",
                                 Mapper.class,
                                 VarIntWritable.class,
                                 FloatArrayWritable.class,
                                 AssembleYTXReducer.class,
                                 VarIntWritable.class,
                                 FloatArrayWritable.class);
    if (assembleJob == null) {
      return null;
    }
    assembleJob.setCombinerClass(AssembleYTXCombiner.class);
    assembleJob.setGroupingComparatorClass(VarIntRawComparator.class);
    assembleJob.setSortComparatorClass(VarIntRawComparator.class);
    assembleJob.setNumReduceTasks(1);
    return assembleJob;
  }

  public static void main(String[] args) throws Exception {
    run(new AssembleYTXStep(), args);
  }

}
