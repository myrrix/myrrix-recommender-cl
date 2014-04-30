/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.util.FastMath;
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.junit.Test;

import net.myrrix.common.MyrrixTest;
import net.myrrix.common.io.IOUtils;
import net.myrrix.store.Namespaces;
import net.myrrix.store.Store;

/**
 * @author Sean Owen
 */
public final class GoldenResultsTest extends MyrrixTest {
  
  private static final int NUM_RECS = 10;
  private static final Pattern COLON = Pattern.compile(":");
  private static final Pattern COMMA_BRACKET = Pattern.compile("[\\[\\],]+");

  @Test
  public void testRecsVsGolden() throws Exception {
    doVsGoldenTest("recommend/", "recs.txt.gz");
  }
  
  @Test
  public void testSimilarityVsGolden() throws Exception {
    doVsGoldenTest("similarItems/", "similarities.txt.gz");
  }
  
  private static void doVsGoldenTest(String hadoopDir, String localFileName) throws IOException {
    Map<Long, List<RecommendedItem>> hadoopRecs = readHadoopValues(hadoopDir, "grouplens10M", 2L);
    
    Map<Long,List<RecommendedItem>> goldenRecs = Maps.newHashMap();
    BufferedReader reader = 
        IOUtils.buffer(IOUtils.openReaderMaybeDecompressing(new File("testdata/" + localFileName)));
    readResultsIntoMap(goldenRecs, reader);
    
    compareExpectedActual(goldenRecs, hadoopRecs);
  }
  
  @Test
  public void testTwoHadoopRecGenerations() throws Exception {
    doTestTwoHadoopGenerations("recommend/");
  }
  
  @Test
  public void testTwoHadoopSimilarityGenerations() throws Exception {
    doTestTwoHadoopGenerations("similarItems/");
  }
  
  private static void doTestTwoHadoopGenerations(String hadoopDir) throws IOException {
    Map<Long, List<RecommendedItem>> hadoopRecs1 = readHadoopValues(hadoopDir, "grouplens10M", 2L);
    Map<Long, List<RecommendedItem>> hadoopRecs2 = readHadoopValues(hadoopDir, "grouplens10M-2", 2L);
    compareExpectedActual(hadoopRecs1, hadoopRecs2);
  }
  
  private static Map<Long,List<RecommendedItem>> readHadoopValues(String hadoopDir, 
                                                                  String dataset,
                                                                  long generation) throws IOException {
    Namespaces.setGlobalBucket("testdata");
    String recsPrefix = Namespaces.getInstanceGenerationPrefix(dataset, generation) + hadoopDir;
    Store store = Store.get();

    Map<Long,List<RecommendedItem>> hadoopRecs = Maps.newHashMap();

    for (String recFilePrefix : store.list(recsPrefix, true)) {
      BufferedReader reader = store.streamFrom(recFilePrefix);
      readResultsIntoMap(hadoopRecs, reader);
    }
    return hadoopRecs;
  }

  private static void compareExpectedActual(Map<Long,List<RecommendedItem>> expected,
                                            Map<Long,List<RecommendedItem>> actual) {
    assertEquals(expected.size(), actual.size());
    assertTrue(expected.keySet().containsAll(actual.keySet()));
    assertTrue(actual.keySet().containsAll(expected.keySet()));
    
    Mean mean = new Mean();
    for (Map.Entry<Long,List<RecommendedItem>> golden : expected.entrySet()) {
      Long id = golden.getKey();
      List<RecommendedItem> expectedRecs = golden.getValue();
      List<RecommendedItem> actualRecs = actual.get(id);
      double score = score(expectedRecs, actualRecs);
      if (score < 0.1) {
        System.out.println(id);
        System.out.println(toIDs(expectedRecs));
        System.out.println(toIDs(actualRecs));
        System.out.println();
      }
      mean.increment(score);
    }

    double result = mean.getResult();
    System.out.println(result);
    assertTrue(String.valueOf(result), result > 0.87);
  }
  
  private static double score(List<RecommendedItem> expected, List<RecommendedItem> actual) {
    double score = 0.0;
    double maxScore = 0.0;
    for (int i = 0; i < actual.size(); i++) {
      long actualID = actual.get(i).getItemID();
      int index = indexIn(actualID, expected);
      double weight = FastMath.pow(2.0, -i-1);
      double indexScore = index < 0 ? 0.0 : FastMath.pow(2.0, -index-1);
      maxScore += weight * weight;
      score += weight * indexScore;
    }
    return score / maxScore;
  }
  
  private static String toIDs(Iterable<RecommendedItem> recs) {
    StringBuilder result = new StringBuilder();
    for (RecommendedItem rec : recs) {
      result.append(String.format("%08d", rec.getItemID())).append('\t');
    }
    return result.toString();
  }
  
  private static int indexIn(long id, List<RecommendedItem> recs) {
    for (int i = 0; i < recs.size(); i++) {
      if (recs.get(i).getItemID() == id) {
        return i;
      }
    }
    return -1;
  }

  private static void readResultsIntoMap(Map<Long, List<RecommendedItem>> hadoopRecs, BufferedReader reader) 
      throws IOException {
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        int tab = line.indexOf('\t');
        Long id = Long.valueOf(line.substring(0, tab));
        List<RecommendedItem> recs = Lists.newArrayListWithCapacity(NUM_RECS);
        hadoopRecs.put(id, recs);
        for (String token : COMMA_BRACKET.split(line.substring(tab + 1))) {
          if (token.isEmpty()) {
            continue;
          }
          String[] idValue = COLON.split(token);
          recs.add(new GenericRecommendedItem(Long.parseLong(idValue[0]), Float.parseFloat(idValue[1])));
        }
        Preconditions.checkState(recs.size() <= NUM_RECS);
      }
    } finally {
      reader.close();
    }
  }

}
