/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.popular;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class PopularUserStep extends AbstractPopularStep {

  @Override
  String getSourceDir() {
    return "itemVectors";
  }

  @Override
  String getPopularPathDir() {
    return "popularUsersByItemPartition";
  }

  public static void main(String[] args) throws Exception {
    run(new PopularUserStep(), args);
  }

}
