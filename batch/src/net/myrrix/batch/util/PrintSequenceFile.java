/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.util;

import java.io.File;
import java.io.PrintStream;

import com.google.common.base.Charsets;
import com.google.common.collect.Ordering;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import net.myrrix.batch.common.PartFilePathFilter;
import net.myrrix.batch.common.iterator.sequencefile.PathType;
import net.myrrix.batch.common.iterator.sequencefile.SequenceFileDirIterable;
import net.myrrix.batch.common.iterator.sequencefile.SequenceFileIterable;

/**
 * Simply prints a sequence file to stdout or to a file, {@code out.txt}, if the -f flag is used.
 * 
 * @author Sean Owen
 * @since 1.0
 */
public final class PrintSequenceFile {

  private PrintSequenceFile() {
  }

  public static void main(String[] args) throws Exception {
    PrintStream out = System.out;
    try {

      for (String path : args) {

        if ("-f".equals(path)) {
          out = new PrintStream(new File("out.txt"), Charsets.UTF_8.name());
          continue;
        }

        out.println(path);
        Configuration conf = new Configuration();

        if (path.endsWith("/")) {

          for (Pair<Writable,Writable> pair :
               new SequenceFileDirIterable<Writable,Writable>(new Path(path),
                                                              PathType.LIST,
                                                              PartFilePathFilter.INSTANCE,
                                                              Ordering.<FileStatus>natural(),
                                                              true,
                                                              conf)) {
            printRecord(out, pair);
          }

        } else {

          for (Pair<Writable,Writable> pair :
               new SequenceFileIterable<Writable,Writable>(new Path(path), conf)) {
            printRecord(out, pair);
          }

        }
        out.println();

      }

    } finally {
      if (!out.equals(System.out)) {
        out.close();
      }
    }
  }

  private static void printRecord(PrintStream out, Pair<Writable, Writable> pair) {
    out.print(pair.getFirst());
    Writable value = pair.getSecond();
    if (value != null) {
      out.print('\t');
      out.print(value);
    }
    out.println();
  }

}
