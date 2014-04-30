/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.myrrix.batch.common.iterator.sequencefile;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import com.google.common.base.Function;
import com.google.common.collect.ForwardingIterator;
import com.google.common.collect.Iterators;
import com.google.common.io.Closer;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Writable;

/**
 * Like {@link SequenceFileIterator}, but iterates not just over one sequence file, but many. The input path
 * may be specified as a directory of files to read, or as a glob pattern. The set of files may be optionally
 * restricted with a {@link PathFilter}.
 * 
 * @author Sean Owen
 * @author Mahout
 * @since 1.0
 * @param <K> file key class
 * @param <V> file value class
 */
public final class SequenceFileDirIterator<K extends Writable,V extends Writable>
    extends ForwardingIterator<Pair<K,V>> implements Closeable {

  private static final FileStatus[] NO_STATUSES = new FileStatus[0];

  private final Iterator<Pair<K,V>> delegate;
  private final Closer closer;

  /**
   * Constructor that uses either {@link FileSystem#listStatus(Path)} or
   * {@link FileSystem#globStatus(Path)} to obtain list of files to iterate over
   * (depending on pathType parameter).
   */
  public SequenceFileDirIterator(Path path,
                                 PathType pathType,
                                 PathFilter filter,
                                 Comparator<FileStatus> ordering,
                                 final boolean reuseKeyValueInstances,
                                 final Configuration conf) throws IOException {
    
    FileStatus[] statuses;
    FileSystem fs = path.getFileSystem(conf);
    if (filter == null) {
      statuses = pathType == PathType.GLOB ? fs.globStatus(path) : fs.listStatus(path);
    } else {
      statuses = pathType == PathType.GLOB ? fs.globStatus(path, filter) : fs.listStatus(path, filter);
    }

    if (statuses == null) {
      statuses = NO_STATUSES;
    } else {
      if (ordering == null) {
        // If order does not matter, use a random order
        Collections.shuffle(Arrays.asList(statuses));
      } else {
        Arrays.sort(statuses, ordering);        
      }
    }
    
    closer = Closer.create();
    
    Iterator<Iterator<Pair<K, V>>> fsIterators =
      Iterators.transform(Iterators.forArray(statuses),
                          new Function<FileStatus, Iterator<Pair<K, V>>>() {
                            @Override
                            public Iterator<Pair<K, V>> apply(FileStatus from) {
                              try {
                                SequenceFileIterator<K, V> iterator = 
                                    new SequenceFileIterator<K, V>(from.getPath(),
                                                                   reuseKeyValueInstances, 
                                                                   conf);
                                closer.register(iterator);
                                return iterator;
                              } catch (IOException ioe) {
                                throw new IllegalStateException(from.getPath().toString(), ioe);
                              }
                            }
                          });

    delegate = Iterators.concat(fsIterators);
  }

  @Override
  protected Iterator<Pair<K,V>> delegate() {
    return delegate;
  }

  @Override
  public void close() throws IOException {
    closer.close();
  }

}
