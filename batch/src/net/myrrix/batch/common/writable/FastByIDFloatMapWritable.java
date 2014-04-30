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

import net.myrrix.common.collection.FastByIDFloatMap;
import net.myrrix.common.io.Varint;

/**
 * A {@link Writable} that encapsulate a {@link FastByIDFloatMap}.
 * 
 * @author Sean Owen
 * @since 1.0
 */
public final class FastByIDFloatMapWritable implements Writable, Cloneable, Serializable {
  
  private FastByIDFloatMap map;
  
  public FastByIDFloatMapWritable() {
  }
  
  public FastByIDFloatMapWritable(FastByIDFloatMap map) {
    this.map = map;
  }
  
  public FastByIDFloatMap getMap() {
    return map;
  }

  public void setMap(FastByIDFloatMap map) {
    this.map = map;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Preconditions.checkNotNull(map);
    Varint.writeUnsignedVarInt(map.size(), dataOutput);
    for (FastByIDFloatMap.MapEntry entry : map.entrySet()) {
      Varint.writeSignedVarLong(entry.getKey(), dataOutput);
      dataOutput.writeFloat(entry.getValue());
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int size = Varint.readUnsignedVarInt(dataInput);
    if (size == 0) {
      map = null;
    } else {
      map = new FastByIDFloatMap(size);
      for (int i = 0; i < size; i++) {
        long id = Varint.readSignedVarLong(dataInput);
        float value = dataInput.readFloat();
        map.put(id, value);
      }
    }
  }
  
  @Override
  public String toString() {
    return String.valueOf(map);
  }
  
  @Override
  public FastByIDFloatMapWritable clone() {
    return new FastByIDFloatMapWritable(map.clone());
  }

}
