/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.popular;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;

import net.myrrix.batch.JobStep;
import net.myrrix.batch.common.writable.FastIDSetWritable;
import net.myrrix.batch.common.writable.VarIntRawComparator;
import net.myrrix.store.Namespaces;

/**
 * @author Sean Owen
 */
abstract class AbstractPopularStep extends JobStep {

  @Override
  protected final Job buildJob() throws IOException {
    String instanceID = getInstanceID();
    long generationID = getGenerationID();
    String tempPrefix = Namespaces.getTempPrefix(instanceID, generationID);
    Job popularStepJob =  prepareJob(tempPrefix + getSourceDir() + '/',
                                     tempPrefix + getPopularPathDir() + '/',
                                     PopularMapper.class,
                                     VarIntWritable.class,
                                     FastIDSetWritable.class,
                                     PopularReducer.class,
                                     VarLongWritable.class,
                                     NullWritable.class);
    if (popularStepJob == null) {
      return null;
    }
    popularStepJob.setGroupingComparatorClass(VarIntRawComparator.class);
    popularStepJob.setSortComparatorClass(VarIntRawComparator.class);
    popularStepJob.setCombinerClass(PopularCombiner.class);
    return popularStepJob;
  }

  abstract String getSourceDir();
  
  abstract String getPopularPathDir();

}
