/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.store;

import java.util.Collection;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;

import net.myrrix.common.ClassUtils;

/**
 * {@link Configuration} subclass which manages Myrrix-specific settings. Subclasses ensure that settings
 * specific to the underlying datastore, like S3, are set as well.
 * 
 * @author Sean Owen
 * @since 1.0
 */
public abstract class MyrrixConfiguration extends Configuration {

  private static final String INSTANCE_ID_KEY = "instanceID";
  private static final String GENERATION_ID_KEY = "generationID";
  private static final String BUCKET_NAME_KEY = "bucketName";

  public static MyrrixConfiguration get() {
    return get(new Configuration());
  }

  public static MyrrixConfiguration get(Configuration configuration) {
    return ClassUtils.loadInstanceOf("net.myrrix.store.DelegateConfiguration",
                                     MyrrixConfiguration.class,
                                     new Class<?>[] { Configuration.class },
                                     new Object[] { configuration });
  }

  protected MyrrixConfiguration(Configuration configuration) {
    super(configuration);
    if (configuration.get("io.file.buffer.size") == null) {
      // Bigger copies. See also setting in JobStep, but using here to make sure local client-to-cluster
      // copies are fast!
      configuration.setInt("io.file.buffer.size", 1 << 17); // ~131K 
    }
    //if (configuration.get("io.bytes.per.checksum") == null) {
    //  configuration.setInt("io.bytes.per.checksum", 1 << 13); // ~8K 
    //}
    copyMyrrixSystemProperties();
  }

  public static Collection<String> getSystemPropertiesToForward() {
    Collection<String> keys = Sets.newHashSet();
    keys.addAll(getSystemPropertiesToForward("batch.*"));
    keys.addAll(getSystemPropertiesToForward("model.*"));
    keys.addAll(getSystemPropertiesToForward("output.*"));
    return keys;
  }

  private static Collection<String> getSystemPropertiesToForward(String keyPattern) {
    Collection<String> expandedKeys = Sets.newHashSet();
    if (keyPattern.endsWith("*")) {
      String prefix = keyPattern.substring(0, keyPattern.length() - 2);
      for (Object systemKey : System.getProperties().keySet()) {
        String systemKeyString = systemKey.toString();
        if (systemKeyString.startsWith(prefix)) {
          expandedKeys.add(systemKeyString);
        }
      }
    } else {
      expandedKeys.add(keyPattern);
    }
    return expandedKeys;
  }

  private void copyMyrrixSystemProperties() {
    for (String key : getSystemPropertiesToForward()) {
      String value = System.getProperty(key);
      if (value != null) {
        set(key, value);
      }
    }
  }

  public final String getInstanceID() {
    String value = get(INSTANCE_ID_KEY);
    Preconditions.checkState(value != null, "No value for key %s", INSTANCE_ID_KEY);
    return value;
  }

  public final void setInstanceID(String instanceID) {
    set(INSTANCE_ID_KEY, instanceID);
  }

  public final long getGenerationID() {
    long value = getLong(GENERATION_ID_KEY, -1L);
    Preconditions.checkState(value != -1L, "No value for key %s", GENERATION_ID_KEY);

    return value;
  }

  public final void setGenerationID(long generationID) {
    setLong(GENERATION_ID_KEY, generationID);
  }

  public final String getBucketName() {
    String value = get(BUCKET_NAME_KEY);
    Preconditions.checkNotNull(value, "No value for key %s", BUCKET_NAME_KEY);
    return value;
  }

  public void setBucketName(String bucketName) {
    set(BUCKET_NAME_KEY, bucketName);
  }

}
