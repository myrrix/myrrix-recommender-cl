/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.store.partition;

import java.util.Collections;
import java.util.List;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.Tag;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.store.S3Utils;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class PartitionBuilderImpl implements PartitionBuilder {

  private static final Logger log = LoggerFactory.getLogger(PartitionBuilderImpl.class);

  private static final String PARTITION_KEY = "myrrix-partition";
  private static final String PORT_KEY = "myrrix-port";
  private static final String BUCKET_KEY = "myrrix-bucket";
  private static final String INSTANCE_KEY = "myrrix-instanceID";
  private static final Filter[]  KEY_FILTERS = {
    new Filter("tag:" + PARTITION_KEY, Collections.singletonList("*")),
  };

  private static Region readRegion() {
    String availabilityZone = S3Utils.readLocalMetadata("placement/availability-zone", "us-east-1");
    char lastChar = availabilityZone.charAt(availabilityZone.length() - 1);
    Preconditions.checkState(lastChar >= 'a' && lastChar <= 'z', "Unrecognized zone: %s", availabilityZone);
    // Chop 'a' off of "eu-west-1a" for example
    return RegionUtils.getRegion(availabilityZone.substring(0, availabilityZone.length() - 1));
  }

  @Override
  public List<List<HostAndPort>> loadPartitions(int defaultPort, String bucket, String instanceID) {

    Region region = readRegion();
    log.debug("Examining region {} for instances", region);

    AmazonEC2Client ec2Client = region.createClient(AmazonEC2Client.class, 
                                                    S3Utils.getAWSCredentialsProvider(), 
                                                    S3Utils.buildClientConfiguration());
    DescribeInstancesResult describeInstancesResult =
        ec2Client.describeInstances(new DescribeInstancesRequest().withFilters(KEY_FILTERS));
    ec2Client.shutdown();

    Multimap<Integer,HostAndPort> partitionReplicas = HashMultimap.create();

    for (Reservation reservation : describeInstancesResult.getReservations()) {
      for (Instance instance : reservation.getInstances()) {

        Iterable<Tag> tags = instance.getTags();
        log.debug("Instance {} has tags {}", instance.getInstanceId(), tags);

        Integer partition = null;
        Integer port = null;
        String taggedBucket = null;
        String taggedInstanceID = null;
        for (Tag tag : tags) {
          String key = tag.getKey();
          String value = tag.getValue();
          if (PARTITION_KEY.equals(key)) {
            partition = Integer.valueOf(value);
          } else if (PORT_KEY.equals(key)) {
            port = Integer.valueOf(value);
          } else if (BUCKET_KEY.equals(key)) {
            taggedBucket = value;
          } else if (INSTANCE_KEY.equals(key)) {
            taggedInstanceID = value;
          }
        }
        if (taggedBucket != null && !taggedBucket.equals(bucket)) {
          continue;
        }
        if (taggedInstanceID != null && !taggedInstanceID.equals(instanceID)) {
          continue;
        }

        Preconditions.checkNotNull(partition, "No partition tag %s", PARTITION_KEY);
        String host = instance.getPublicDnsName();
        partitionReplicas.put(partition, HostAndPort.fromParts(host, port == null ? defaultPort : port));
      }
    }

    Preconditions.checkState(!partitionReplicas.isEmpty(),
                             "No partitions were found running on EC2; is this running on EC2?");

    List<Integer> partitions = Lists.newArrayList(partitionReplicas.keySet());
    Collections.sort(partitions);
    int lastPartition = -1;
    for (int partition : partitions) {
      Preconditions.checkState(lastPartition + 1 == partition, "Missing partition %s", partition);
      lastPartition = partition;
    }

    List<List<HostAndPort>> result = Lists.newArrayListWithCapacity(lastPartition + 1);
    for (int partition = 0; partition <= lastPartition; partition++) {
      List<HostAndPort> replicas = Lists.newArrayList(partitionReplicas.get(partition));
      log.debug("Partition {} replicas: {}", partition, replicas);
      result.add(replicas);
    }
    return result;
  }

}
