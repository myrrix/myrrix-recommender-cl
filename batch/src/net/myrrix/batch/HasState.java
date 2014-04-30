/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch;

import java.io.IOException;
import java.util.Collection;

/**
 * Package-private interface that simply unifies a few classes that need to be able to return
 * {@link MyrrixStepState}s.
 *
 * @author Sean Owen
 */
interface HasState {

  /**
   * @return one or more {@link MyrrixStepState}s representing the state or states managed by the object.
   */
  Collection<MyrrixStepState> getStepStates() throws IOException;

}
