/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.common.writable;

import org.apache.hadoop.io.RawComparator;
import org.apache.mahout.math.VarIntWritable;

/**
 * An efficient {@link RawComparator} for {@link VarIntWritable}.
 *
 * @author Sean Owen
 * @since 1.0
 */
public final class VarIntRawComparator implements RawComparator<VarIntWritable> {

  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    int a = readSignedVarInt(b1, s1, l1);
    int b = readSignedVarInt(b2, s2, l2);
    if (a < b) {
      return -1;
    }
    if (a > b) {
      return 1;
    }
    return 0;
  }

  @Override
  public int compare(VarIntWritable a, VarIntWritable b) {
    return a.compareTo(b);
  }

  private static int readSignedVarInt(byte[] bytes, int start, int length) {
    int raw = readUnsignedVarInt(bytes, start, length);
    int temp = (((raw << 31) >> 31) ^ raw) >> 1;
    return temp ^ (raw & (1 << 31));
  }

  private static int readUnsignedVarInt(byte[] bytes, int start, int length) {
    //Preconditions.checkArgument(length >= 1 && length <= 5);
    int value = 0;
    int i = 0;
    int maxMinus1 = start + length - 1;
    for (int offset = start; offset < maxMinus1; offset++) {
      int b = bytes[offset];
      //Preconditions.checkArgument(b < 0);
      value |= (b & 0x7F) << i;
      i += 7;
    }
    int b = bytes[maxMinus1];
    //Preconditions.checkArgument(b >= 0);
    return value | (b << i);
  }

}
