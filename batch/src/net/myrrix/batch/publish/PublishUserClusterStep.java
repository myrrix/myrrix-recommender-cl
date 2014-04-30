/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.publish;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class PublishUserClusterStep extends PublishClusterStep {

  public static void main(String[] args) throws Exception {
    run(new PublishUserClusterStep(), args);
  }

  @Override
  boolean isX() {
    return true;
  }

}
