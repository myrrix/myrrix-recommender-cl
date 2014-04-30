/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.merge;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.math.VarLongWritable;

import net.myrrix.batch.JobStep;
import net.myrrix.batch.common.writable.FastByIDFloatMapWritable;
import net.myrrix.batch.common.writable.VarLongRawComparator;
import net.myrrix.store.Namespaces;

/**
 * @author Sean Owen
 */
abstract class AbstractToVectorsStep<M extends Mapper<?,?,VarLongWritable,EntityPrefWritable>> extends JobStep {

  @Override
  protected final Job buildJob() throws IOException {

    String instanceID = getInstanceID();
    long generationID = getGenerationID();

    Job vectorJob = prepareJob(Namespaces.getInputPrefix(instanceID, generationID),
                               Namespaces.getTempPrefix(instanceID, generationID) + getSuffix(),
                               getMapper(),
                               VarLongWritable.class,
                               EntityPrefWritable.class,
                               ToVectorReducer.class,
                               VarLongWritable.class,
                               FastByIDFloatMapWritable.class);
    if (vectorJob == null) {
      return null;
    }
    vectorJob.setGroupingComparatorClass(VarLongRawComparator.class);
    vectorJob.setSortComparatorClass(VarLongRawComparator.class);
    return vectorJob;
  }
  
  abstract Class<M> getMapper();
  
  abstract String getSuffix();

}
