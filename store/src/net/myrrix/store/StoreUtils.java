/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.store;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Utilities that are generally used but not quite core storage operations as in {@link Store}.
 * 
 * @author Sean Owen
 * @since 1.0
 */
public final class StoreUtils {

  private StoreUtils() {
  }

  /**
   * Lists all generation keys for a given instance. This skips any system directories, for example.
   * 
   * @param instanceID instance to retrieve generations for
   * @return locations of all generation directories for the given instance
   */
  public static List<String> listGenerationsForInstance(String instanceID) throws IOException {
    String prefix = Namespaces.getInstancePrefix(instanceID);
    List<String> rawGenerations = Store.get().list(prefix, false);
    Iterator<String> it = rawGenerations.iterator();
    String sysPrefix = Namespaces.getSysPrefix(instanceID);
    while (it.hasNext()) {
      if (it.next().startsWith(sysPrefix)) {
        it.remove();
      }
    }
    return rawGenerations;
  }
  
}
