/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch;

import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Encapsulates an Amazon instance type.
 * 
 * @author Sean Owen
 */
enum InstanceType {
  
  // See http://aws.amazon.com/ec2/instance-types/  
  // See http://docs.amazonwebservices.com/ElasticMapReduce/latest/DeveloperGuide/TaskConfiguration_AMI2.3.html
  
  M1_SMALL("m1.small", 1700, 1),
  M1_MEDIUM("m1.medium", 3750, 1),
  M1_LARGE("m1.large", 7500, 1),
  M1_XLARGE("m1.xlarge", 15000, 3),
  M2_XLARGE("m2.xlarge", 17100, 1),
  M2_2XLARGE("m2.2xlarge", 34200, 2),
  M2_4XLARGE("m2.4xlarge", 68400, 4),
  //M3_XLARGE("m3.xlarge", 15000, ?),
  //M3_2XLARGE("m3.2xlarge", 30000, ?),
  C1_MEDIUM("c1.medium", 1700, 1),
  C1_XLARGE("c1.xlarge", 7000, 2),
  CC1_4XLARGE("cc1.4xlarge", 23000, 3),
  CC2_8XLARGE("cc2.8xlarge", 60500, 6),
  CG1_4XLARGE("cg1.4xlarge", 22000, 3),
  //CR1_8XLARGE("cr1.8xlarge", 244000, ??),
  HI1_4XLARGE("hi1.4xlarge", 60500, 6),
  HS1_8XLARGE("hs1.8xlarge", 117000, 6);
  
  private static final Map<String,InstanceType> BY_INSTANCE_NAME;
  static {
    InstanceType[] values = values();
    BY_INSTANCE_NAME = Maps.newHashMapWithExpectedSize(values.length);
    for (InstanceType type : values) {
      BY_INSTANCE_NAME.put(type.getInstanceName(), type);
    }
  }
  
  private final String instanceName;
  private final int ramMB;
  private final int reducers;
  
  InstanceType(String instanceName, int ramMB, int reducers) {
    Preconditions.checkNotNull(instanceName);
    Preconditions.checkArgument(ramMB > 0);
    Preconditions.checkArgument(reducers > 0);
    this.instanceName = instanceName;
    this.ramMB = ramMB;
    this.reducers = reducers;
  }
  
  static InstanceType forInstanceName(String name) {
    return BY_INSTANCE_NAME.get(name);
  }

  String getInstanceName() {
    return instanceName;
  }

  int getRamMB() {
    return ramMB;
  }

  int getReducers() {
    return reducers;
  }
  
  @Override
  public String toString() {
    return instanceName;
  }
  
}
