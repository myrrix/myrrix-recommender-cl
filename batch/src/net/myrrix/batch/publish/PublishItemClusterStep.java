/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.publish;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class PublishItemClusterStep extends PublishClusterStep {

  public static void main(String[] args) throws Exception {
    run(new PublishItemClusterStep(), args);
  }

  @Override
  boolean isX() {
    return false;
  }

}
