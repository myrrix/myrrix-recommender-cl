/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.store;

import java.io.IOException;
import java.net.URI;
import java.net.URL;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.common.io.IOUtils;

/**
 * Utility methods related to Amazon S3.
 *
 * @author Sean Owen
 * @since 1.0
 */
public final class S3Utils {

  private static final Logger log = LoggerFactory.getLogger(S3Utils.class);

  private static final String METADATA_URL_ROOT = "http://169.254.169.254/latest/meta-data/";

  private S3Utils() {
  }
  
  public static AWSCredentialsProvider getAWSCredentialsProvider() {
    return new AWSCredentialsProviderChain(
        new MyrrixSysPropertyCredentialsProvider(),
        new MyrrixEnvVariableCredentialsProvider(),
        new SystemPropertiesCredentialsProvider(),
        new EnvironmentVariableCredentialsProvider());
  }

  /**
   * @param path key or "path" into EC2 metadata to query
   * @param defaultValue value to return if no metadata exists for the key
   * @return value of metadata at that path, from the EC2 environment, as a string
   */
  public static String readLocalMetadata(String path, String defaultValue) {
    try {
      URL ec2PlacementURL = URI.create(METADATA_URL_ROOT + path).toURL();
      String value = IOUtils.readSmallTextFromURL(ec2PlacementURL);
      log.debug("Metadata path {} has value {}", path, value);
      return value;
    } catch (IOException ioe) {
      log.warn("Can't load meta data from {}{} ({}); is this running on EC2? Using default of {}",
               METADATA_URL_ROOT, path, ioe.toString(), defaultValue);
      return defaultValue;
    }
  }
  
  public static ClientConfiguration buildClientConfiguration() {
    // Add max connections to be "safe" -- would rather not fail, but if it gets this big we have a problem
    // Also double timeout to account for large-ish delays in the network
    ClientConfiguration clientConfiguration = new ClientConfiguration();
    clientConfiguration.setSocketTimeout(2 * clientConfiguration.getSocketTimeout());
    clientConfiguration.setMaxConnections(3 * clientConfiguration.getMaxConnections());
    return clientConfiguration;
  }

}
