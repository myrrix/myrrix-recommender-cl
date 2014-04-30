/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.merge.tag;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class MergeNewOldItemTagsStep extends MergeNewOldTagsStep {

  @Override
  public boolean isX() {
    return true;
  }
  
  public static void main(String[] args) throws Exception {
    run(new MergeNewOldItemTagsStep(), args);
  }

}
