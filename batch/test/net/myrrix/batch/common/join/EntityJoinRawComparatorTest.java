/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.common.join;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

import net.myrrix.common.MyrrixTest;

/**
 * Tests {@link EntityJoinRawComparator}.
 */
public final class EntityJoinRawComparatorTest extends MyrrixTest {

  @Test
  public void testCompare() throws IOException {
    doTest(Long.MIN_VALUE, Long.MAX_VALUE);
    doTest(0, 0);
    doTest(0, Long.MAX_VALUE);
    doTest(0, 1);
    doTest(-1, 0);
    doTest(Long.MIN_VALUE, 0);
  }

  @Test
  public void testCompareWithOrder() throws IOException {
    doTest(Long.MIN_VALUE, Long.MAX_VALUE, EntityJoinKey.AFTER, EntityJoinKey.BEFORE);
    doTest(0, 0, EntityJoinKey.AFTER, EntityJoinKey.BEFORE);
    doTest(0, Long.MAX_VALUE, EntityJoinKey.MIDDLE, EntityJoinKey.MIDDLE);
    doTest(0, 1, EntityJoinKey.AFTER, EntityJoinKey.MIDDLE);
    doTest(-1, 0, EntityJoinKey.AFTER, EntityJoinKey.MIDDLE);
    doTest(Long.MIN_VALUE, 0, EntityJoinKey.AFTER, EntityJoinKey.BEFORE);
    doTest(Long.MIN_VALUE, Long.MIN_VALUE, EntityJoinKey.BEFORE, EntityJoinKey.BEFORE);
    doTest(Long.MIN_VALUE, Long.MIN_VALUE, EntityJoinKey.BEFORE, EntityJoinKey.MIDDLE);
    doTest(Long.MIN_VALUE, Long.MIN_VALUE, EntityJoinKey.BEFORE, EntityJoinKey.AFTER);
  }

  private static void doTest(long aValue, long bValue) throws IOException {
    doTest(aValue, bValue, EntityJoinKey.BEFORE, EntityJoinKey.BEFORE);
  }

  private static void doTest(long aValue, long bValue, int aOrder, int bOrder) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);

    EntityJoinKey a = new EntityJoinKey(aValue, aOrder);
    EntityJoinKey b = new EntityJoinKey(bValue, bOrder);

    a.write(out);
    int bStart = baos.size();
    b.write(out);
    int bLength = baos.size() - bStart;

    out.close();
    byte[] bytes = baos.toByteArray();

    int expectedOrder;
    if (aValue == bValue) {
      aOrder = 0xFF & aOrder;
      bOrder = 0xFF & bOrder;
      expectedOrder = aOrder < bOrder ? -1 : aOrder > bOrder ? 1 : 0;
    } else {
      expectedOrder = aValue < bValue ? -1 : aValue > bValue ? 1 : 0;
    }

    assertEquals(expectedOrder,
                 new EntityJoinRawComparator().compare(bytes, 0, bStart, bytes, bStart, bLength));
    assertEquals(-expectedOrder,
                 new EntityJoinRawComparator().compare(bytes, bStart, bLength, bytes, 0, bStart));
  }

}
