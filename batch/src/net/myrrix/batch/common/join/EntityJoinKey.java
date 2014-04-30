/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.common.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;

import net.myrrix.common.io.Varint;

/**
 * <p>This {@link WritableComparable} is used where two or more different types of values,
 * which are both keyed by some entity value (user ID or item ID), come from different
 * {@link org.apache.hadoop.mapreduce.Mapper}s and need to be joined in one
 * {@link org.apache.hadoop.mapreduce.Reducer}. This key encapsulates the entity ID,
 * and also a numeric "join order" value which distinguishes values from the different sources.
 * It also determines the ordering, for one key ID, in which the values will be seen in the
 * {@link org.apache.hadoop.mapreduce.Reducer}.</p>
 *
 * <p>The values still need to be of the same {@link org.apache.hadoop.io.Writable} class of course.</p>
 *
 * <p>However it's necessary to ensure that before/after keys for one entity don't get sent to different
 * {@link org.apache.hadoop.mapreduce.Reducer}s. To do this, a caller must call
 * {@link org.apache.hadoop.mapreduce.Job#setPartitionerClass(Class)} with
 * {@link KeyPartitioner} which will ensure that keys with the same entity go to the same partition.</p>
 * 
 * @author Sean Owen
 * @since 1.0
 */
public final class EntityJoinKey implements WritableComparable<EntityJoinKey>, Cloneable, Serializable {

  public static final int BEFORE = 0;
  public static final int MIDDLE = 0x7F;
  public static final int AFTER = 0xFF;

  private long key;
  private int joinOrder;

  public EntityJoinKey() {
  }

  public EntityJoinKey(long key, int joinOrder) {
    this.key = key;
    this.joinOrder = joinOrder;
  }

  public long getKey() {
    return key;
  }

  public void setKey(long key) {
    this.key = key;
  }

  public int getJoinOrder() {
    return joinOrder;
  }

  public void setJoinOrder(int joinOrder) {
    Preconditions.checkArgument(joinOrder >= BEFORE && joinOrder <= AFTER);
    this.joinOrder = joinOrder;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Varint.writeSignedVarLong(key, out);
    out.writeByte(joinOrder);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    key = Varint.readSignedVarLong(in);
    joinOrder = in.readByte() & 0xFF;
  }

  @Override
  public int hashCode() {
    return Longs.hashCode(key) ^ joinOrder;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof EntityJoinKey)) {
      return false;
    }
    EntityJoinKey otherKey = (EntityJoinKey) other;
    return key == otherKey.key && joinOrder == otherKey.joinOrder;
  }

  @Override
  public String toString() {
    return key + "(" + joinOrder + ')';
  }

  @Override
  public int compareTo(EntityJoinKey other) {
    if (key < other.key) {
      return -1;
    }
    if (key > other.key) {
      return 1;
    }
    return joinOrder - other.joinOrder;
  }
  
  @Override
  public EntityJoinKey clone() {
    return new EntityJoinKey(key, joinOrder);
  }

  public static final class KeyPartitioner extends Partitioner<EntityJoinKey,Object> {
    @Override
    public int getPartition(EntityJoinKey key, Object value, int numPartitions) {
      return ((int) key.getKey() & Integer.MAX_VALUE) % numPartitions;
    }
  }

}
