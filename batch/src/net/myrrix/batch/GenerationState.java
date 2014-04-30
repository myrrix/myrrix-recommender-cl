/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch;

import java.io.Serializable;
import java.net.URI;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * Encapsulates the current state of a run of {@link GenerationRunner}.
 *
 * @author Sean Owen
 * @since 1.0
 */
public final class GenerationState implements Serializable {

  private final long generationID;
  private final MyrrixJobState jobState;
  private final boolean running;
  private final Date startTime;
  private final Date endTime;

  public GenerationState(long generationID,
                         MyrrixJobState jobState,
                         boolean running,
                         Date startTime,
                         Date endTime) {
    this.generationID = generationID;
    this.jobState = jobState;
    this.running = running;
    this.startTime = clone(startTime);
    this.endTime = clone(endTime);
  }

  private static Date clone(Date d) {
    return d == null ? null : new Date(d.getTime());
  }

  public long getGenerationID() {
    return generationID;
  }

  public boolean isRunning() {
    return running;
  }

  public List<MyrrixStepState> getStepStates() {
    return jobState == null ? Collections.<MyrrixStepState>emptyList() : jobState.getStepStates();
  }

  public MyrrixStepStatus getStatus() {
    return jobState == null ? MyrrixStepStatus.PENDING : jobState.getStatus();
  }

  /**
   * @return the URI of some human-readable management console for the current generation, or {@code null}
   *  if no generation is running
   */
  public URI getManagementURI() {
    return jobState == null ? null : jobState.getManagementURI();
  }

  /**
   * @return time that the generation start or {@code null} if not started
   */
  public Date getStartTime() {
    return clone(startTime);
  }

  /**
   * @return time that the generation ended or {@code null} if not completed
   */
  public Date getEndTime() {
    return clone(endTime);
  }

  @Override
  public String toString() {
    return generationID + ":" + jobState;
  }

}
