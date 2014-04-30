/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.recommend;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.math.VarLongWritable;

import net.myrrix.batch.JobStep;
import net.myrrix.batch.common.writable.VarLongRawComparator;
import net.myrrix.common.ClassUtils;
import net.myrrix.store.Namespaces;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class CollectRecommendStep extends JobStep {

  @Override
  protected Job buildJob() throws IOException {

    String instanceID = getInstanceID();
    long generationID = getGenerationID();

    @SuppressWarnings("unchecked")    
    Job recommendJob = prepareJob(Namespaces.getTempPrefix(instanceID, generationID) + "partialRecommend/",
                                  Namespaces.getInstanceGenerationPrefix(instanceID, generationID) + "recommend/",
                                  Mapper.class,
                                  VarLongWritable.class,
                                  EntityPrefWritable.class,
                                  CollectRecommendReducer.class,
                                  Text.class,
                                  Text.class);
    if (recommendJob == null) {
      return null;
    }
    recommendJob.setGroupingComparatorClass(VarLongRawComparator.class);
    recommendJob.setSortComparatorClass(VarLongRawComparator.class);

    String customOutputFormatClass = System.getProperty("batch.output.recommend.customFormat");
    String customOutputPath = System.getProperty("batch.output.recommend.customPath");
    Preconditions.checkArgument((customOutputFormatClass == null) == (customOutputPath == null),
                                "batch.output.recommend.{customFormat,customPath} must be set together");

    if (customOutputFormatClass != null && customOutputPath != null) {
      recommendJob.setOutputFormatClass(ClassUtils.loadClass(customOutputFormatClass, OutputFormat.class));
      FileOutputFormat.setOutputPath(recommendJob, new Path(customOutputPath));
    } else {
      // Default:
      setUseTextOutput(recommendJob);
    }

    return recommendJob;
  }

  public static void main(String[] args) throws Exception {
    run(new CollectRecommendStep(), args);
  }

}
