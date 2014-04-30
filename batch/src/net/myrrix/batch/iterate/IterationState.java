/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.iterate;

/**
 * Encapsulates the parameters of an iteration step, like which iteration number it is.
 * 
 * @author Sean Owen
 * @since 1.0
 */
public final class IterationState {
  
  private final boolean computingX;
  private final int iteration;
  private final String iterationKey;
  
  IterationState(boolean computingX, int iteration, String iterationKey) {
    this.computingX = computingX;
    this.iteration = iteration;
    this.iterationKey = iterationKey;
  }

  public boolean isComputingX() {
    return computingX;
  }

  public int getIteration() {
    return iteration;
  }
  
  public String getIterationKey() {
    return iterationKey;
  }
  
  @Override
  public String toString() {
    return (computingX ? 'X' : 'Y') + " / " + iteration + " / " + iterationKey;
  }

}
