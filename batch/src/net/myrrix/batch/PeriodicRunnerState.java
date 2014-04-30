/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch;

import java.io.Serializable;
import java.net.URI;
import java.util.Date;
import java.util.List;

/**
 * Encapsulates the current state of a run of {@link PeriodicRunner}.
 *
 * @author Sean Owen
 * @since 1.0
 */
public final class PeriodicRunnerState implements Serializable {

  private final List<GenerationState> generationStates;
  private final boolean running;
  private final Date nextScheduledRun;
  private final int currentGenerationMB;

  PeriodicRunnerState(List<GenerationState> generationStates,
                      boolean running,
                      Date nextScheduledRun,
                      int currentGenerationMB) {
    this.generationStates = generationStates;
    this.running = running;
    this.nextScheduledRun = clone(nextScheduledRun);
    this.currentGenerationMB = currentGenerationMB;
  }

  private static Date clone(Date d) {
    return d == null ? null : new Date(d.getTime());
  }

  public List<GenerationState> getGenerationStates() {
    return generationStates;
  }

  public boolean isRunning() {
    return running;
  }

  /**
   * @return time when next run is scheduled to start, or {@code null} if not scheduled
   */
  public Date getNextScheduledRun() {
    return clone(nextScheduledRun);
  }

  public int getCurrentGenerationMB() {
    return currentGenerationMB;
  }

  /**
   * @return the URI of some human-readable management console for the current generation, or {@code null}
   *  if no generation is running
   */
  public URI getManagementURI() {
    return generationStates.isEmpty() ? null : generationStates.get(0).getManagementURI();
  }

  @Override
  public String toString() {
    return generationStates.toString();
  }

}
