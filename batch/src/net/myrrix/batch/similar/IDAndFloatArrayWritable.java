/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.similar;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.VarLongWritable;

import net.myrrix.batch.common.writable.FloatArrayWritable;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class IDAndFloatArrayWritable implements Writable, Cloneable, Serializable {

  private VarLongWritable idWritable;
  private FloatArrayWritable floatArrayWritable;

  public IDAndFloatArrayWritable() {
  }

  public IDAndFloatArrayWritable(long id, float[] values) {
    this.idWritable = new VarLongWritable(id);
    this.floatArrayWritable = new FloatArrayWritable(values);
  }

  private IDAndFloatArrayWritable(VarLongWritable idWritable, FloatArrayWritable floatArrayWritable) {
    this.idWritable = idWritable;
    this.floatArrayWritable = floatArrayWritable;
  }

  public long getID() {
    return idWritable.get();
  }

  public float[] getValues() {
    return floatArrayWritable.get();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    idWritable.write(dataOutput);
    floatArrayWritable.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    idWritable = new VarLongWritable();
    idWritable.readFields(dataInput);
    floatArrayWritable = new FloatArrayWritable();
    floatArrayWritable.readFields(dataInput);
  }

  @Override
  public String toString() {
    return idWritable + "/" + floatArrayWritable;
  }
  
  @Override
  public IDAndFloatArrayWritable clone() {
    return new IDAndFloatArrayWritable(new VarLongWritable(idWritable.get()),
                                       floatArrayWritable.clone());
  }

}
