/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.iterate.cluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

import net.myrrix.batch.common.writable.FloatArrayWritable;
import net.myrrix.common.io.Varint;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class WeightedFloatArrayWritable implements Writable, Cloneable, Serializable {

  private int weight;
  private FloatArrayWritable floatArrayWritable;

  public WeightedFloatArrayWritable() {
  }

  public WeightedFloatArrayWritable(float[] values) {
    this(1, values);
  }

  public WeightedFloatArrayWritable(int weight, float[] values) {
    this.weight = weight;
    this.floatArrayWritable = new FloatArrayWritable(values);
  }

  public int getWeight() {
    return weight;
  }

  public float[] getValues() {
    return floatArrayWritable.get();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Varint.writeUnsignedVarInt(weight, dataOutput);
    floatArrayWritable.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    weight = Varint.readUnsignedVarInt(dataInput);
    floatArrayWritable = new FloatArrayWritable();
    floatArrayWritable.readFields(dataInput);
  }

  @Override
  public String toString() {
    return weight + "/" + floatArrayWritable;
  }
  
  @Override
  public WeightedFloatArrayWritable clone() {
    return new WeightedFloatArrayWritable(weight, floatArrayWritable.get().clone());
  }

}
