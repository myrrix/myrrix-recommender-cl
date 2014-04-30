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

import net.myrrix.common.io.Varint;

/**
 * A {@link Writable} encapsulating a simple {@code float[]}.
 * 
 * @author Sean Owen
 * @since 1.0
 */
public final class FloatArrayWritable implements Writable, Cloneable, Serializable {

  private static final float[] EMPTY = new float[0];

  private float[] vector;

  public FloatArrayWritable() {
  }

  public FloatArrayWritable(float[] vector) {
    this.vector = vector;
  }

  public float[] get() {
    return vector;
  }
  
  public void set(float[] vector) {
    this.vector = vector;
  }
  
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    float[] vector = this.vector;
    Preconditions.checkNotNull(vector);
    int dimension = vector.length;
    Varint.writeUnsignedVarInt(dimension, dataOutput);
    for (float f : vector) {
      dataOutput.writeFloat(f);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int dimension = Varint.readUnsignedVarInt(dataInput);
    if (dimension == 0) {
      this.vector = EMPTY;
    } else {
      float[] vector = new float[dimension];
      for (int i = 0; i < dimension; i++) {
        vector[i] = dataInput.readFloat();
      }
      this.vector = vector;
    }
  }

  @Override
  public String toString() {
    float[] vector = this.vector;
    if (vector == null || vector.length == 0) {
      return "[]";
    }
    StringBuilder result = new StringBuilder();
    result.append('[');
    for (float f : vector) {
      result.append(f).append('\t');
    }
    result.setCharAt(result.length() - 1, ']');
    return result.toString();
  }
  
  @Override
  public FloatArrayWritable clone() {
    return new FloatArrayWritable(vector.clone());
  }

}
