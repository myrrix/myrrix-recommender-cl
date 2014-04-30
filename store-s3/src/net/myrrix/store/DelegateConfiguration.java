/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.store;

import com.amazonaws.auth.AWSCredentials;
import org.apache.hadoop.conf.Configuration;

/**
 * Implementation specialized for the Amazon S3 environment.
 * 
 * @author Sean Owen
 * @since 1.0
 */
public final class DelegateConfiguration extends MyrrixConfiguration {

  private static final String S3_ACCESS_KEY = "fs.s3.awsAccessKeyId";
  private static final String S3_SECRET_KEY = "fs.s3.awsSecretAccessKey";
  private static final String S3N_ACCESS_KEY = "fs.s3n.awsAccessKeyId";
  private static final String S3N_SECRET_KEY = "fs.s3n.awsSecretAccessKey";

  public DelegateConfiguration(Configuration configuration) {
    super(configuration);
    if (get(S3_ACCESS_KEY) == null || get(S3_SECRET_KEY) == null ||
        get(S3N_ACCESS_KEY) == null || get(S3N_SECRET_KEY) == null) {
      AWSCredentials credentials = S3Utils.getAWSCredentialsProvider().getCredentials();
      String accessKey = credentials.getAWSAccessKeyId();
      String secretKey = credentials.getAWSSecretKey();
      set(S3_ACCESS_KEY, accessKey);
      set(S3_SECRET_KEY, secretKey);
      set(S3N_ACCESS_KEY, accessKey);
      set(S3N_SECRET_KEY, secretKey);
    }
    // Hack for now to identify when we're on AWS, to work around bug above    
    set("myrrix.environment", "emr");
  }

  @Override
  public void setBucketName(String bucketName) {
    super.setBucketName(bucketName);
    String s3FS = "s3://" + bucketName;
    set("fs.defaultFS", s3FS);
    set("fs.default.name", s3FS);
  }

}
