/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.publish;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class PublishUserCentroidsStep extends PublishCentroidsStep {

  public static void main(String[] args) throws Exception {
    run(new PublishUserCentroidsStep(), args);
  }

  @Override
  boolean isX() {
    return true;
  }

}
