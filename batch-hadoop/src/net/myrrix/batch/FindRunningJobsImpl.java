/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.store.MyrrixConfiguration;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class FindRunningJobsImpl implements FindRunningJobs {

  private static final Logger log = LoggerFactory.getLogger(FindRunningJobsImpl.class);

  @Override
  public List<String> find(String instanceID) throws IOException {
    String prefix = "Myrrix-" + instanceID;
    List<String> result = Lists.newArrayList();
    JobClient client = new JobClient(new JobConf(MyrrixConfiguration.get()));
    JobStatus[] statuses = client.getAllJobs();
    if (statuses != null) {
      for (JobStatus jobStatus : statuses) {
        int state = jobStatus.getRunState();
        if (state == JobStatus.RUNNING || state == JobStatus.PREP) {
          RunningJob runningJob = client.getJob(jobStatus.getJobID());
          String jobName = runningJob.getJobName();
          log.info("Found running job {}", jobName);
          if (jobName.startsWith(prefix)) {
            result.add(jobName);
          }
        }
      }
    }
    return result;
  }

}
