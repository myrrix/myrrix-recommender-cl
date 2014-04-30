/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.merge.tag;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class MergeNewOldUserTagsStep extends MergeNewOldTagsStep {

  @Override
  public boolean isX() {
    return false;
  }
  
  public static void main(String[] args) throws Exception {
    run(new MergeNewOldUserTagsStep(), args);
  }

}
