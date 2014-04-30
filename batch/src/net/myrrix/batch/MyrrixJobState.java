/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch;

import java.io.Serializable;
import java.net.URI;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

import com.google.common.collect.Lists;

/**
 * Encapsulates the state of an entire Hadoop job, composed of several states.
 *
 * @author Sean Owen
 * @since 1.0
 * @see MyrrixStepState
 */
public final class MyrrixJobState implements Serializable {

  private URI managementURI;
  private final List<MyrrixStepState> stepStates;

  MyrrixJobState() {
    stepStates = Lists.newArrayList();
  }

  /**
   * @return the URI of some human-readable management console for the current job, or {@code null}
   *  if no job is running yet or the management console is not available
   */
  public URI getManagementURI() {
    return managementURI;
  }

  void setManagementURI(URI managementURI) {
    this.managementURI = managementURI;
  }

  public MyrrixStepStatus getStatus() {
    if (stepStates.isEmpty()) {
      return MyrrixStepStatus.PENDING;
    }
    Collection<MyrrixStepStatus> observedStates = EnumSet.noneOf(MyrrixStepStatus.class);
    for (MyrrixStepState stepState : stepStates) {
      observedStates.add(stepState.getStatus());
    }
    for (MyrrixStepStatus clearStatuses :
         new MyrrixStepStatus[] { MyrrixStepStatus.FAILED, MyrrixStepStatus.CANCELLED, MyrrixStepStatus.RUNNING }) {
      if (observedStates.contains(clearStatuses)) {
        return clearStatuses;
      }
    }
    // Only pending and completed now
    if (observedStates.contains(MyrrixStepStatus.PENDING)) {
      if (observedStates.contains(MyrrixStepStatus.COMPLETED)) {
        return MyrrixStepStatus.RUNNING;
      }
      return MyrrixStepStatus.PENDING;
    }
    return MyrrixStepStatus.COMPLETED;
  }

  /**
   * @return states of the job's constituent steps, as {@link MyrrixStepState} objects. These are in order
   *  by their execution order in the job
   */
  public List<MyrrixStepState> getStepStates() {
    return stepStates;
  }

  void addStepState(MyrrixStepState newStepState) {
    stepStates.add(newStepState);
  }

  void addStepStates(Collection<MyrrixStepState> newStepStates) {
    stepStates.addAll(newStepStates);
  }

  @Override
  public String toString() {
    return stepStates.toString();
  }

}
