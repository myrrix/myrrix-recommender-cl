/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.iterate;

import net.myrrix.batch.JobStep;
import net.myrrix.store.Namespaces;

/**
 * Superclass of {@link JobStep}s that are run many times, iteratively
 * 
 * @author Sean Owen
 * @since 1.0
 */
public abstract class IterationStep extends JobStep {

  private IterationState iterationState;

  @Override
  protected String getCustomJobName() {
    IterationState iterationState = getIterationState();
    StringBuilder name = new StringBuilder(100);
    name.append("Myrrix-").append(getInstanceID());
    name.append('-').append(getGenerationID());
    name.append('-').append(iterationState.getIteration());
    name.append('-').append(iterationState.isComputingX() ? 'X' : 'Y');
    name.append('-').append(getClass().getSimpleName());
    return name.toString();
  }

  protected final IterationState getIterationState() {
    if (iterationState != null) {
      return iterationState;
    }
    int currentIteration = getIteration();
    String iterationsKey = Namespaces.getIterationsPrefix(getInstanceID(), getGenerationID()) + currentIteration + '/';
    iterationState = new IterationState(isComputingX(), currentIteration, iterationsKey);
    return iterationState;
  }
  
}
