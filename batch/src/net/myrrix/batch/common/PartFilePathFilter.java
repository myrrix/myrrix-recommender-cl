/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.common;

import java.io.File;
import java.io.FilenameFilter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * Selects all "part-" files within a directory. This is the default prefix of file names created
 * by Hadoop.
 *
 * @author Sean Owen
 * @since 1.0
 */
public final class PartFilePathFilter implements PathFilter, FilenameFilter {

  public static final PartFilePathFilter INSTANCE = new PartFilePathFilter();

  private PartFilePathFilter() {
  }

  @Override
  public boolean accept(Path path) {
    return accept(path.getName());
  }

  @Override
  public boolean accept(File file, String filename) {
    return accept(filename);
  }

  private static boolean accept(String filename) {
    return filename.startsWith("part-");
  }

}
