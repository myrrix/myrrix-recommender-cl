/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.recommend;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

import net.myrrix.batch.common.writable.FastIDSetWritable;
import net.myrrix.batch.similar.IDAndFloatArrayWritable;
import net.myrrix.common.collection.FastIDSet;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class IDAndKnownItemsAndFloatArrayWritable implements Writable, Cloneable, Serializable {

  private IDAndFloatArrayWritable idAndFloatArrayWritable;
  private FastIDSetWritable knownItemsWritable;

  public IDAndKnownItemsAndFloatArrayWritable() {
  }

  public IDAndKnownItemsAndFloatArrayWritable(long id, float[] values, FastIDSet knownItems) {
    this.idAndFloatArrayWritable = new IDAndFloatArrayWritable(id, values);
    this.knownItemsWritable = new FastIDSetWritable(knownItems);
  }

  private IDAndKnownItemsAndFloatArrayWritable(IDAndFloatArrayWritable idAndFloatArrayWritable,
                                               FastIDSetWritable knownItemsWritable) {
    this.idAndFloatArrayWritable = idAndFloatArrayWritable;
    this.knownItemsWritable = knownItemsWritable;
  }

  public long getID() {
    return idAndFloatArrayWritable.getID();
  }

  public float[] getValues() {
    return idAndFloatArrayWritable.getValues();
  }

  public FastIDSet getKnownItems() {
    return knownItemsWritable.getIDs();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    idAndFloatArrayWritable.write(dataOutput);
    knownItemsWritable.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    idAndFloatArrayWritable = new IDAndFloatArrayWritable();
    idAndFloatArrayWritable.readFields(dataInput);
    knownItemsWritable = new FastIDSetWritable();
    knownItemsWritable.readFields(dataInput);
  }

  @Override
  public String toString() {
    return idAndFloatArrayWritable + "/" + knownItemsWritable;
  }
  
  @Override
  public IDAndKnownItemsAndFloatArrayWritable clone() {
    return new IDAndKnownItemsAndFloatArrayWritable(idAndFloatArrayWritable.clone(), knownItemsWritable.clone());
  }

}
