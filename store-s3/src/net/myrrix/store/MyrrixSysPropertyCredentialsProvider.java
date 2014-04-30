/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.store;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

/**
 * Derived from {@link com.amazonaws.auth.SystemPropertiesCredentialsProvider}.
 * 
 * @author Sean Owen
 * @author Amazon
 * @since 1.0
 */
public final class MyrrixSysPropertyCredentialsProvider implements AWSCredentialsProvider {

  public static final String ACCESS_KEY_PROPERTY = "store.aws.accessKey";
  public static final String SECRET_KEY_PROPERTY = "store.aws.secretKey";

  @Override
  public AWSCredentials getCredentials() {
    String accessKey = System.getProperty(ACCESS_KEY_PROPERTY);
    String secretKey = System.getProperty(SECRET_KEY_PROPERTY);
    if (accessKey != null && secretKey != null) {
      return new BasicAWSCredentials(accessKey, secretKey);
    }
    throw new AmazonClientException("Unable to load AWS credentials from Java system properties " +
                                    '(' + ACCESS_KEY_PROPERTY + " and " + SECRET_KEY_PROPERTY + ')');
  }

  @Override
  public void refresh() {
    // do nothing
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
