/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.store;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

final class FilesOrDirsPathFilter implements PathFilter {

  private final FileSystem fs;
  private final boolean files;

  FilesOrDirsPathFilter(FileSystem fs, boolean files) {
    this.fs = fs;
    this.files = files;
  }

  @Override
  public boolean accept(Path maybeListPath) {
    try {
      return !maybeListPath.toString().endsWith("_SUCCESS") && (fs.isFile(maybeListPath) == files);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
