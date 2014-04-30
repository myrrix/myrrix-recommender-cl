/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.merge;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class ToItemVectorsStep extends AbstractToVectorsStep<TransposeUserItemMapper> {

  @Override
  Class<TransposeUserItemMapper> getMapper() {
    return TransposeUserItemMapper.class;
  }

  @Override
  String getSuffix() {
    return "itemVectors/";
  }

  public static void main(String[] args) throws Exception {
    run(new ToItemVectorsStep(), args);
  }
  
}
