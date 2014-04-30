/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.store;

import java.net.URI;

import org.apache.hadoop.fs.FileSystem;

/**
 * An implementations specialized for HDFS.
 * 
 * @author Sean Owen
 * @since 1.0
 */
public final class DelegateNamespaces extends Namespaces {

  private final String prefix;

  public DelegateNamespaces(String bucket) {
    super(bucket);
    URI defaultURI = FileSystem.getDefaultUri(MyrrixConfiguration.get());
    String host = defaultURI.getHost();
    int port = defaultURI.getPort();
    if (port > 0) {
      prefix = "hdfs://" + host + ':' + port + '/';
    } else {
      prefix = "hdfs://" + host + '/';
    }
  }

  @Override
  public String getBucketPath() {
    return prefix + getBucket() + '/';
  }

}
