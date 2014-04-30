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
public final class MyrrixEnvVariableCredentialsProvider implements AWSCredentialsProvider {

  public static final String ACCESS_KEY_ENV_VAR = "AWS_ACCESS_KEY";
  public static final String SECRET_KEY_ENV_VAR = "AWS_SECRET_KEY";

  @Override
  public AWSCredentials getCredentials() {
    String accessKey = System.getenv(ACCESS_KEY_ENV_VAR);
    String secretKey = System.getenv(SECRET_KEY_ENV_VAR);
    if (accessKey != null && secretKey != null) {
      return new BasicAWSCredentials(accessKey, secretKey);
    }
    throw new AmazonClientException("Unable to load AWS credentials from environment variables " +
                                    '(' + ACCESS_KEY_ENV_VAR + " and " + SECRET_KEY_ENV_VAR + ')');
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
