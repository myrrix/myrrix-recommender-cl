/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.zip.GZIPInputStream;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.VarLongWritable;

import net.myrrix.batch.common.iterator.sequencefile.PathType;
import net.myrrix.batch.common.iterator.sequencefile.SequenceFileDirIterable;
import net.myrrix.batch.common.writable.FloatArrayWritable;
import net.myrrix.common.LangUtils;
import net.myrrix.common.collection.FastByIDMap;
import net.myrrix.common.collection.FastIDSet;
import net.myrrix.common.io.IOUtils;
import net.myrrix.store.Namespaces;

/**
 * Utility methods meant for Mappers and Reducers, which is a different environment than the 
 * {@link net.myrrix.batch.JobStep}s. Here we can't use classes like {@link net.myrrix.store.Store} 
 * due to AWS complications.
 * 
 * @author Sean Owen
 * @since 1.0
 */
public final class DataUtils {
  
  private DataUtils() {
  }
  
  /**
   * @param prefix location of a directory on distributed storage, whose files are read for keys
   * @param conf {@link Configuration} appropriate to environment's distribute storage system
   * @return {@link FastIDSet} of all keys (IDs) found in files under the prefix
   */
  public static FastIDSet readKeysFromTextFile(String prefix, Configuration conf) throws IOException {
    FastIDSet keys = new FastIDSet();
    if (prefix == null) {
      return keys;
    }

    Path path = Namespaces.toPath(prefix);
    FileSystem fs = path.getFileSystem(conf);

    if (!fs.exists(path)) {
      return keys;
    }
    Preconditions.checkState(fs.getFileStatus(path).isDir(), "Not a directory: %s", path);

    FileStatus[] statuses = fs.listStatus(path, PartFilePathFilter.INSTANCE);
    // Shuffle statuses to read them in a different order than perhaps other concurrent readers
    Collections.shuffle(Arrays.asList(statuses));

    for (Path filePath : FileUtil.stat2Paths(statuses)) {
      // Assuming all are compressed with gzip; all text output is gzip by default:
      BufferedReader reader = IOUtils.bufferStream(new GZIPInputStream(fs.open(filePath)));
      try {
        String line;
        while ((line = reader.readLine()) != null) {
          keys.add(Long.parseLong(line));
        }
      } finally {
        reader.close();
      }
    }
    return keys;
  }  
  
  public static FastByIDMap<float[]> loadPartialY(int partition,
                                                  int numPartitions,
                                                  Path path,
                                                  FastIDSet tagIDs,
                                                  Configuration conf) {
    FastByIDMap<float[]> result = new FastByIDMap<float[]>(10000);
    for (Pair<VarLongWritable,FloatArrayWritable> record :
         new SequenceFileDirIterable<VarLongWritable,FloatArrayWritable>(path, 
                                                                         PathType.LIST,
                                                                         PartFilePathFilter.INSTANCE,
                                                                         conf)) {
      long id = record.getFirst().get();
      if (!tagIDs.contains(id) && LangUtils.mod(id, numPartitions) == partition) {
        result.put(id, record.getSecond().get());
      }
    }
    return result;
  }
  
}
