/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch;

import java.io.IOException;
import java.util.List;

/**
 * @author Sean Owen
 * @since 1.0
 */
public interface FindRunningJobs {

  /**
   * @param instanceID instance in question
   * @return name of Myrrix-related jobs for this instance that appear to be current running on the cluster
   */
  List<String> find(String instanceID) throws IOException;

}
