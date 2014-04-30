/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.similar;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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
public final class SimilarStep extends JobStep {

  @Override
  protected Job buildJob() throws IOException {

    String instanceID = getInstanceID();
    long generationID = getGenerationID();

    Job similarJob = prepareJob(Namespaces.getTempPrefix(instanceID, generationID) + "distributeSimilar/",
                                Namespaces.getInstanceGenerationPrefix(instanceID, generationID) + "similarItems/",
                                null,
                                VarLongWritable.class,
                                EntityPrefWritable.class,
                                SimilarReducer.class,
                                Text.class,
                                Text.class);

    if (similarJob == null) {
      return null;
    }
    similarJob.setGroupingComparatorClass(VarLongRawComparator.class);
    similarJob.setSortComparatorClass(VarLongRawComparator.class);

    String customOutputFormatClass = System.getProperty("batch.output.similar.customFormat");
    String customOutputPath = System.getProperty("batch.output.similar.customPath");
    Preconditions.checkArgument((customOutputFormatClass == null) == (customOutputPath == null),
                                "batch.output.similar.{customFormat,customPath} must be set together");

    if (customOutputFormatClass != null && customOutputPath != null) {
      similarJob.setOutputFormatClass(ClassUtils.loadClass(customOutputFormatClass, OutputFormat.class));
      FileOutputFormat.setOutputPath(similarJob, new Path(customOutputPath));
    } else {
      // Default:
      setUseTextOutput(similarJob);
    }

    return similarJob;
  }

  public static void main(String[] args) throws Exception {
    run(new SimilarStep(), args);
  }

}
