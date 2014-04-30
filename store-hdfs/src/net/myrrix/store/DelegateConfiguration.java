/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.store;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * An implementation specialized for Hadoop and its configuration files.
 * 
 * @author Sean Owen
 * @since 1.0
 */
public final class DelegateConfiguration extends MyrrixConfiguration {

  private static final String HADOOP_HOME_KEY = "HADOOP_HOME";
  private static final String HADOOP_CONF_DIR_KEY = "HADOOP_CONF_DIR";

  public DelegateConfiguration(Configuration configuration) {
    super(configuration);
    setFromEnvOrConf("core-site.xml", Collections.singleton(Arrays.asList("fs.defaultFS", "fs.default.name")));
    setFromEnvOrConf("mapred-site.xml", Collections.singleton(Collections.singleton("mapred.job.tracker")));
    configuration.set("mapreduce.framework.name", "classic");
  }

  /**
   * @param fileName config file to fall back to, if System properties do not specify all values
   * @param keySynonymGroupsToSet several groups of keys whose values should be set. Within each group, if a
   *  value is found for any key, it is set to all keys in the group. This is repeated for each group. System
   *  properties are queried first, and if any group has no value, the given config file is loaded
   *  (potentially overwriting values set by System properties).
   */
  private void setFromEnvOrConf(String fileName, Collection<? extends Collection<String>> keySynonymGroupsToSet) {
    Preconditions.checkArgument(!keySynonymGroupsToSet.isEmpty());
    Collection<Collection<String>> keySynonymGroupsStillToSet = Lists.newArrayList();
    
    for (Collection<String> synonymGroup : keySynonymGroupsToSet) {
      String value = null;
      for (String synonym : synonymGroup) {
        value = System.getProperty(synonym);
        if (value != null) {
          break;
        }
      }
      if (value == null) {
        keySynonymGroupsStillToSet.add(synonymGroup);
      } else {
        for (String synonym : synonymGroup) {
          set(synonym, value);
        }
      }
    }
    
    if (!keySynonymGroupsStillToSet.isEmpty()) {
      
      File hadoopConfDir = findHadoopConfDir();
      File configFile = new File(hadoopConfDir, fileName);
      Preconditions.checkState(configFile.exists() && configFile.isFile(), "No such config file: %s", configFile);
      
      addResource(new Path(configFile.toString()));
      
      for (Collection<String> synonymGroup : keySynonymGroupsStillToSet) {
        String value = null;
        for (String synonym : synonymGroup) {
          value = get(synonym);
          if (value != null) {
            break;
          }
        }
        Preconditions.checkNotNull(value, "None of %s specified, and not set in %s", synonymGroup, configFile);
        for (String synonym : synonymGroup) {
          set(synonym, value);
        }
      }
    }
  }

  private static File findHadoopConfDir() {
    String hadoopConfPath = System.getenv(HADOOP_CONF_DIR_KEY);
    if (hadoopConfPath != null) {
      File hadoopConfDir = new File(hadoopConfPath);
      Preconditions.checkState(hadoopConfDir.exists() && hadoopConfDir.isDirectory(),
                               "Not a directory: %s", hadoopConfDir);
      return hadoopConfDir;
    }
    String hadoopHomePath = System.getenv(HADOOP_HOME_KEY);
    if (hadoopHomePath != null) {
      File hadoopHomeDir = new File(hadoopHomePath);
      File hadoopConfDir = new File(hadoopHomeDir, "conf");
      if (hadoopConfDir.exists() && hadoopConfDir.isDirectory()) {
        return hadoopConfDir;
      }
      Preconditions.checkState(hadoopHomeDir.exists() && hadoopHomeDir.isDirectory(),
                               "Neither is a directory: %s / %s", hadoopConfDir, hadoopHomeDir);
      return hadoopHomeDir;
    }
    throw new IllegalStateException("Neither " + HADOOP_HOME_KEY + " nor " + HADOOP_CONF_DIR_KEY +
                                    " specifies local Hadoop config");
  }

}
