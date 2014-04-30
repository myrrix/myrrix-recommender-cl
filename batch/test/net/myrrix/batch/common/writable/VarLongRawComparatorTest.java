/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.common.writable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.mahout.math.VarLongWritable;
import org.junit.Test;

import net.myrrix.common.MyrrixTest;

public final class VarLongRawComparatorTest extends MyrrixTest {

  @Test
  public void testCompare() throws IOException {
    doTest(Long.MIN_VALUE, Long.MAX_VALUE);
    doTest(0, 0);
    doTest(0, Long.MAX_VALUE);
    doTest(0, 1);
    doTest(-1, 0);
    doTest(Long.MIN_VALUE, 0);
  }

  private static void doTest(long aValue, long bValue) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    VarLongWritable a = new VarLongWritable(aValue);
    VarLongWritable b = new VarLongWritable(bValue);
    a.write(out);
    int bStart = baos.size();
    b.write(out);
    int bLength = baos.size() - bStart;
    out.close();
    byte[] bytes = baos.toByteArray();
    int expectedOrder = aValue < bValue ? -1 : aValue > bValue ? 1 : 0;
    assertEquals(expectedOrder,
                 new VarLongRawComparator().compare(bytes, 0, bStart, bytes, bStart, bLength));
    assertEquals(-expectedOrder,
                 new VarLongRawComparator().compare(bytes, bStart, bLength, bytes, 0, bStart));
  }

}
