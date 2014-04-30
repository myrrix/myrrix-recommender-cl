/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.publish;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class PublishYStep extends PublishStep {

  public static void main(String[] args) throws Exception {
    run(new PublishYStep(), args);
  }

  @Override
  boolean isX() {
    return false;
  }

}
