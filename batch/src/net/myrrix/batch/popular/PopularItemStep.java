/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.popular;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class PopularItemStep extends AbstractPopularStep {

  @Override
  String getSourceDir() {
    return "userVectors";
  }

  @Override
  String getPopularPathDir() {
    return "popularItemsByUserPartition";
  }

  public static void main(String[] args) throws Exception {
    run(new PopularItemStep(), args);
  }

}
