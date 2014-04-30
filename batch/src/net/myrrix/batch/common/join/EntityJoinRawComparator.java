/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.common.join;

import org.apache.hadoop.io.RawComparator;

/**
 * An efficient {@link RawComparator} for {@link EntityJoinKey}.
 *
 * @author Sean Owen
 * @since 1.0
 */
public final class EntityJoinRawComparator implements RawComparator<EntityJoinKey> {

  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    long a = readSignedVarLong(b1, s1, l1 - 1);
    long b = readSignedVarLong(b2, s2, l2 - 1);
    if (a < b) {
      return -1;
    }
    if (a > b) {
      return 1;
    }
    int aOrder = b1[s1 + l1 - 1] & 0xFF;
    int bOrder = b2[s2 + l2 - 1] & 0xFF;
    if (aOrder < bOrder) {
      return -1;
    }
    if (aOrder > bOrder) {
      return 1;
    }
    return 0;
  }

  @Override
  public int compare(EntityJoinKey a, EntityJoinKey b) {
    return a.compareTo(b);
  }

  // From Mahout's Varint:

  private static long readSignedVarLong(byte[] bytes, int start, int length) {
    long raw = readUnsignedVarLong(bytes, start, length);
    long temp = (((raw << 63) >> 63) ^ raw) >> 1;
    return temp ^ (raw & (1L << 63));
  }

  private static long readUnsignedVarLong(byte[] bytes, int start, int length) {
    //Preconditions.checkArgument(length >= 1 && length <= 10);
    long value = 0L;
    int i = 0;
    int maxMinus1 = start + length - 1;
    for (int offset = start; offset < maxMinus1; offset++) {
      long b = bytes[offset];
      //Preconditions.checkArgument(b < 0);
      value |= (b & 0x7F) << i;
      i += 7;
    }
    long b = bytes[maxMinus1];
    //Preconditions.checkArgument(b >= 0);
    return value | (b << i);
  }

}
