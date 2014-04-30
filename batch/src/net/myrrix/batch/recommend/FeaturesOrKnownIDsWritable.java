/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.recommend;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import com.google.common.base.Preconditions;
import org.apache.hadoop.io.Writable;

import net.myrrix.batch.common.writable.FastIDSetWritable;
import net.myrrix.batch.common.writable.FloatArrayWritable;
import net.myrrix.common.collection.FastIDSet;

/**
 * A {@link Writable} that holds either a {@link FloatArrayWritable} or a {@link FastIDSetWritable}
 * but not both.
 *
 * @author Sean Owen
 * @since 1.0
 */
public final class FeaturesOrKnownIDsWritable implements Writable, Cloneable, Serializable {

  private FloatArrayWritable features;
  private FastIDSetWritable knownIDs;

  public float[] getFeatures() {
    Preconditions.checkState(features != null && knownIDs == null);
    return features.get();
  }

  public void setFeatures(float[] features) {
    setFeatures(new FloatArrayWritable(features));
  }

  public void setFeatures(FloatArrayWritable features) {
    this.features = features;
    this.knownIDs = null;
  }

  public FastIDSet getKnownIDs() {
    Preconditions.checkState(features == null && knownIDs != null);
    return knownIDs.getIDs();
  }

  public void setKnownIDs(FastIDSet knownIDs) {
    setKnownIDs(new FastIDSetWritable(knownIDs));
  }

  public void setKnownIDs(FastIDSetWritable knownIDs) {
    this.features = null;
    this.knownIDs = knownIDs;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Preconditions.checkState((features == null) != (knownIDs == null),
                             "Exactly one of features and knownIDs should be set");
    boolean writeFeatures = features != null;
    dataOutput.writeBoolean(writeFeatures);
    if (writeFeatures) {
      features.write(dataOutput);
    } else {
      knownIDs.write(dataOutput);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    boolean readFeatures = dataInput.readBoolean();
    if (readFeatures) {
      FloatArrayWritable newFeatures = new FloatArrayWritable();
      newFeatures.readFields(dataInput);
      setFeatures(newFeatures);
    } else {
      FastIDSetWritable newKnownIDs = new FastIDSetWritable();
      newKnownIDs.readFields(dataInput);
      setKnownIDs(newKnownIDs);
    }
  }
  
  @Override
  public String toString() {
    return features == null ? knownIDs.toString() : features.toString();
  }
  
  @Override
  public FeaturesOrKnownIDsWritable clone() {
    FeaturesOrKnownIDsWritable clone = new FeaturesOrKnownIDsWritable();
    if (features == null) {
      clone.setKnownIDs(knownIDs.clone());
    } else {
      clone.setFeatures(features.clone());
    }
    return clone;
  }

}
