/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch;

/**
 * Generic exception that is thrown when a Hadoop job fails.
 *
 * @author Sean Owen
 * @since 1.0
 */
public final class JobException extends Exception {

  public JobException() {
  }

  public JobException(String message) {
    super(message);
  }

  public JobException(Throwable cause) {
    super(cause);
  }

  public JobException(String message, Throwable cause) {
    super(message, cause);
  }

}
