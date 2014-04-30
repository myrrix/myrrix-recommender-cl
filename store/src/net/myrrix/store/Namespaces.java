/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.store;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.fs.Path;

import net.myrrix.common.ClassUtils;

/**
 * Subclasses represent resource names in the context of a specific storage system, like 
 * Amazon S3 or HDFS.
 * 
 * @author Sean Owen
 * @since 1.0
 */
public abstract class Namespaces {

  private static Namespaces instance = null;

  private final String bucket;

  protected Namespaces(String bucket) {
    Preconditions.checkNotNull(bucket);
    this.bucket = bucket;
  }

  public static Namespaces get() {
    Preconditions.checkNotNull(instance, "No namespace configured");
    return instance;
  }

  public static void setGlobalBucket(String bucketName) {
    Preconditions.checkNotNull(bucketName);
    if (instance == null) {
      instance = loadNamespacesInstance(bucketName);
    } else {
      Preconditions.checkState(bucketName.equals(instance.getBucket()),
                               "Namespaces already set to %s; trying to set to %s", instance.getBucket(), bucketName);
    }
  }

  private static Namespaces loadNamespacesInstance(String bucketName) {
    return ClassUtils.loadInstanceOf("net.myrrix.store.DelegateNamespaces",
                                     Namespaces.class,
                                     new Class<?>[] { String.class },
                                     new Object[] { bucketName });
  }

  public final String getBucket() {
    return bucket;
  }

  public abstract String getBucketPath();

  public static String getSysPrefix(String instanceID) {
    return getInstancePrefix(instanceID) + "sys/";
  }

  /**
   * @return key where JAR of MapReduce jobs is stored remotely
   */
  public static String getMyrrixJarPrefix(String instanceID) {
    return getSysPrefix(instanceID) + "myrrix.jar";
  }

  /**
   * @return key where remote JAR containing a {@code RescorerProvider} is optionally stored
   */
  public static String getRescorerJarPrefix(String instanceID) {
    return getSysPrefix(instanceID) + "rescorer.jar";
  }
  
  /**
   * @return key where remote JAR containing a {@code Runnable} / {@link java.io.Closeable} is optionally stored
   */
  public static String getClientThreadJarPrefix(String instanceID) {
    return getSysPrefix(instanceID) + "clientthread.jar";
  }

  /**
   * @return key where remote keystore file is optionally stored
   */
  public static String getKeystoreFilePrefix(String instanceID) {
    return getSysPrefix(instanceID) + "keystore.ks";
  }

  public static Path toPath(String prefix) {
    return new Path(get().getBucketPath() + prefix);
  }

  public static String getInboundPrefix(String instanceID, long generationID) {
    return getInstanceGenerationPrefix(instanceID, generationID) + "inbound/";
  }

  public static String getInputPrefix(String instanceID, long generationID) {
    return getInstanceGenerationPrefix(instanceID, generationID) + "input/";
  }

  public static String getLogsPrefix(String instanceID, long generationID) {
    return getInstanceGenerationPrefix(instanceID, generationID) + "logs/";
  }

  public static String getInstancePrefix(String instanceID) {
    return instanceID + '/';
  }

  public static String getInstanceGenerationPrefix(String instanceID, long generationID) {
    Preconditions.checkArgument(generationID >= 0L, "Bad generation %s", generationID);
    String paddedGenerationID = Strings.padStart(Long.toString(generationID), 10, '0');
    return getInstancePrefix(instanceID) + paddedGenerationID + '/';
  }

  public static String getTempPrefix(String instanceID, long generationID) {
    return getInstanceGenerationPrefix(instanceID, generationID) + "tmp/";
  }

  public static String getIterationsPrefix(String instanceID, long generationID) {
    return getTempPrefix(instanceID, generationID) + "iterations/";
  }

  public static String getGenerationDoneKey(String instanceID, long generationID) {
    return getInstanceGenerationPrefix(instanceID, generationID) + "_SUCCESS";
  }

}
