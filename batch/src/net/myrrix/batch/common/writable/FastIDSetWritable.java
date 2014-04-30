/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.common.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import com.google.common.base.Preconditions;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;

import net.myrrix.common.collection.FastIDSet;
import net.myrrix.common.io.Varint;

/**
 * A {@link Writable} encapsulating a simple {@link FastIDSet}.
 * 
 * @author Sean Owen
 * @since 1.0
 */
public final class FastIDSetWritable implements Writable, Cloneable, Serializable {

  private FastIDSet ids;

  public FastIDSetWritable() {
  }

  public FastIDSetWritable(FastIDSet ids) {
    this.ids = ids;
  }

  public FastIDSet getIDs() {
    return ids;
  }

  public void setIDs(FastIDSet ids) {
    this.ids = ids;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Preconditions.checkNotNull(ids);
    Varint.writeUnsignedVarInt(ids.size(), dataOutput);
    LongPrimitiveIterator it = ids.iterator();
    while (it.hasNext()) {
      Varint.writeSignedVarLong(it.nextLong(), dataOutput);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int size = Varint.readUnsignedVarInt(dataInput);
    if (size == 0) {
      ids = null;
    } else {
      ids = new FastIDSet(size);
      for (int i = 0; i < size; i++) {
        ids.add(Varint.readSignedVarLong(dataInput));
      }
    }
  }

  @Override
  public String toString() {
    return String.valueOf(ids);
  }
  
  @Override
  public FastIDSetWritable clone() {
    return new FastIDSetWritable(ids.clone());
  }

}
