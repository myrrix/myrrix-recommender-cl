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

import com.google.common.collect.AbstractIterator;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * <p>{@link java.util.Iterator} over a {@link SequenceFile}'s keys and values, as a {@link Pair}
 * containing key and value.</p>
 * 
 * @author Sean Owen
 * @author Mahout
 * @since 1.0
 * @param <K> file key class
 * @param <V> file value class
 */
public final class SequenceFileIterator<K extends Writable,V extends Writable>
  extends AbstractIterator<Pair<K,V>> implements Closeable {

  private final SequenceFile.Reader reader;
  private final Configuration conf;
  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private final boolean noValue;
  private K key;
  private V value;
  private final boolean reuseKeyValueInstances;

  /**
   * @throws IOException if path can't be read, or its key or value class can't be instantiated
   */
  public SequenceFileIterator(Path path, boolean reuseKeyValueInstances, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    reader = new SequenceFile.Reader(fs, path, conf);
    this.conf = conf;
    @SuppressWarnings("unchecked")
    Class<K> theKeyClass = (Class<K>) reader.getKeyClass();
    keyClass = theKeyClass;
    @SuppressWarnings("unchecked")
    Class<V> theValueClass = (Class<V>) reader.getValueClass();
    valueClass = theValueClass;
    noValue = NullWritable.class.equals(valueClass);
    this.reuseKeyValueInstances = reuseKeyValueInstances;
  }

  public Class<K> getKeyClass() {
    return keyClass;
  }

  public Class<V> getValueClass() {
    return valueClass;
  }

  @Override
  public void close() throws IOException {
    reader.close();
    endOfData();
  }

  @Override
  protected Pair<K,V> computeNext() {
    if (!reuseKeyValueInstances || value == null) {
      key = ReflectionUtils.newInstance(keyClass, conf);
      if (!noValue) {
        value = ReflectionUtils.newInstance(valueClass, conf);
      }
    }
    try {
      boolean available;
      if (noValue) {
        available = reader.next(key);
      } else {
        available = reader.next(key, value);
      }
      if (!available) {
        close();
        return null;
      }
      return new Pair<K,V>(key, value);
    } catch (IOException ioe) {
      try {
        close();
      } catch (IOException ignored) {
        // continue
      }
      throw new IllegalStateException(ioe);
    }
  }

}
