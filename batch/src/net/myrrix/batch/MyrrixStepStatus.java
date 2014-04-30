/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch;

/**
 * Simplified enumeration of possible states of a step -- or job -- in a Myrrix Hadoop job.
 *
 * @author Sean Owen
 * @since 1.0
 */
public enum MyrrixStepStatus {

  /** Not yet started running. */
  PENDING,
  /** Running normally now. */
  RUNNING,
  /** Stopped running due to an error. */
  FAILED,
  /** Stopped running before completion because of system or user request. */
  CANCELLED,
  /** Completed execution normally and is no longer running. */
  COMPLETED,

}
