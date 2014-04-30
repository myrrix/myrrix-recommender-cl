/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsResult;
import com.amazonaws.services.elasticmapreduce.model.JobFlowDetail;
import com.amazonaws.services.elasticmapreduce.model.JobFlowExecutionState;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.store.S3Utils;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class FindRunningJobsImpl implements FindRunningJobs {

  private static final Logger log = LoggerFactory.getLogger(FindRunningJobsImpl.class);

  @Override
  public List<String> find(String instanceID) throws IOException {
    AWSCredentialsProvider credentialsProvider = S3Utils.getAWSCredentialsProvider();
    ClientConfiguration clientConfiguration = S3Utils.buildClientConfiguration();
    Region region = RegionUtils.getRegion(System.getProperty(DelegateJob.BATCH_EMR_ZONE_KEY, "us-east-1"));    
    AmazonElasticMapReduceClient emrClient = 
        region.createClient(AmazonElasticMapReduceClient.class, credentialsProvider, clientConfiguration);

    String prefix = "Myrrix-" + instanceID;
    try {
      // Will fetch jobs in state RUNNING, WAITING, SHUTTING_DOWN, STARTING
      DescribeJobFlowsRequest request = new DescribeJobFlowsRequest();
      request.setJobFlowStates(Arrays.asList(JobFlowExecutionState.BOOTSTRAPPING.name(),
                                             JobFlowExecutionState.STARTING.name(),
                                             JobFlowExecutionState.WAITING.name(),
                                             JobFlowExecutionState.RUNNING.name(),
                                             JobFlowExecutionState.SHUTTING_DOWN.name()));
      DescribeJobFlowsResult jobFlows = emrClient.describeJobFlows(request);
      List<String> result = Lists.newArrayList();
      for (JobFlowDetail detail : jobFlows.getJobFlows()) {
        String jobName = detail.getName();
        log.info("Found running job {}", jobName);
        if (jobName.startsWith(prefix)) {
          result.add(jobName);
        }
      }
      return result;
    } catch (AmazonClientException ace) {
      throw new IOException(ace);
    } finally {
      emrClient.shutdown();
    }
  }

}
