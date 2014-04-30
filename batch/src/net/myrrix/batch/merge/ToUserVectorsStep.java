/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.merge;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class ToUserVectorsStep extends AbstractToVectorsStep<IdentityUserItemMapper> {

  @Override
  Class<IdentityUserItemMapper> getMapper() {
    return IdentityUserItemMapper.class;
  }

  @Override
  String getSuffix() {
    return "userVectors/";
  }

  public static void main(String[] args) throws Exception {
    run(new ToUserVectorsStep(), args);
  }
  
}
