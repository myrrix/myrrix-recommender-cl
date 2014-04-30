/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch;

import java.io.Serializable;
import java.net.URI;
import java.util.Date;

import com.google.common.base.Preconditions;

/**
 * Encapsulates the status of a single step's state.
 *
 * @author Sean Owen
 * @since 1.0
 * @see MyrrixJobState
 */
public final class MyrrixStepState implements Serializable {

  private final Date startTime;
  private final Date endTime;
  private final String name;
  private final MyrrixStepStatus status;
  private URI managementURI;
  private float mapProgress;
  private float reduceProgress;

  MyrrixStepState(Date startTime,
                  Date endTime,
                  String name,
                  MyrrixStepStatus status) {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(status);
    this.startTime = clone(startTime);
    this.endTime = clone(endTime);
    this.name = name;
    this.status = status;
    mapProgress = Float.NaN;
    reduceProgress = Float.NaN;
  }

  private static Date clone(Date d) {
    return d == null ? null : new Date(d.getTime());
  }

  /**
   * @return time that the step started or {@code null} if not started
   */
  public Date getStartTime() {
    return clone(startTime);
  }

  /**
   * @return time that the step ended or {@code null} if not completed
   */
  public Date getEndTime() {
    return clone(endTime);
  }

  /**
   * @return the name of the step, as used by the cluster
   */
  public String getName() {
    return name;
  }

  /**
   * @return current status of the step, like {@link MyrrixStepStatus#RUNNING}
   */
  public MyrrixStepStatus getStatus() {
    return status;
  }

  /**
   * @return the URI of some human-readable management console for the current step, or {@code null}
   *  if not running or no management console is available
   */
  public URI getManagementURI() {
    return managementURI;
  }

  public void setManagementURI(URI managementURI) {
    this.managementURI = managementURI;
  }

  /**
   * @return step's mapper progress, as value in [0,1]
   */
  public float getMapProgress() {
    return mapProgress;
  }

  void setMapProgress(float mapProgress) {
    this.mapProgress = mapProgress;
  }

  /**
   * @return step's reducer progress, as value in [0,1]
   */
  public float getReduceProgress() {
    return reduceProgress;
  }

  void setReduceProgress(float reduceProgress) {
    this.reduceProgress = reduceProgress;
  }

  @Override
  public String toString() {
    return name + ':' + status;
  }

}
