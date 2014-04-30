/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.store;

/**
 * Implementation specialized for Amazon S3.
 * 
 * @author Sean Owen
 * @since 1.0
 */
public final class DelegateNamespaces extends Namespaces {

  public DelegateNamespaces(String bucket) {
    super(bucket);
  }

  @Override
  public String getBucketPath() {
    return "s3://" + getBucket() + '/';
  }

}
