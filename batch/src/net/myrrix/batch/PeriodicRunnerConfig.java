/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch;

import java.io.Serializable;

import com.google.common.base.Preconditions;

/**
 * Encapsulates configuration for {@link PeriodicRunner}.
 *
 * @author Sean Owen
 * @since 1.0
 */
public final class PeriodicRunnerConfig implements Serializable {

  public static final int NO_THRESHOLD = Integer.MIN_VALUE;

  private String instanceID;
  private String bucket;
  private int dataThresholdMB;
  private int initialTimeThresholdMin;
  private int timeThresholdMin;
  private boolean recommend;
  private boolean makeItemSimilarity;
  private boolean cluster;
  private boolean forceRun;

  /**
   * @return bucket name under which to run Computation Layer. Used with {@link #getInstanceID()}.
   */
  public String getBucket() {
    return bucket;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  /**
   * @return instance ID that the Computation Layer. Used with {@link #getBucket()}.
   */
  public String getInstanceID() {
    return instanceID;
  }

  public void setInstanceID(String instanceID) {
    this.instanceID = instanceID;
  }

  /**
   * @return if not {@link #NO_THRESHOLD}, then after this number of MB are written to the current
   *  generation, a new one will be run
   */
  public int getDataThresholdMB() {
    return dataThresholdMB;
  }

  public void setDataThresholdMB(int dataThresholdMB) {
    Preconditions.checkArgument(dataThresholdMB == NO_THRESHOLD || dataThresholdMB > 0,
                                "Bad data threshold: %s", dataThresholdMB);
    this.dataThresholdMB = dataThresholdMB;
  }

  /**
   * @return the delay before the initial run, in minutes
   * @see #getTimeThresholdMin()
   */
  public int getInitialTimeThresholdMin() {
    return initialTimeThresholdMin;
  }

  public void setInitialTimeThresholdMin(int initialTimeThresholdMin) {
    Preconditions.checkArgument(initialTimeThresholdMin == NO_THRESHOLD || initialTimeThresholdMin >= 0,
                                "Bad initial time threshold: %s", initialTimeThresholdMin);
    this.initialTimeThresholdMin = initialTimeThresholdMin;
  }

  /**
   * @return if not {@link #NO_THRESHOLD}, then if this number of minutes elapses since the last
   *  generation, a new one will be run
   * @see #getInitialTimeThresholdMin()
   */
  public int getTimeThresholdMin() {
    return timeThresholdMin;
  }

  public void setTimeThresholdMin(int timeThresholdMin) {
    Preconditions.checkArgument(timeThresholdMin == NO_THRESHOLD || timeThresholdMin >= 0,
                                "Bad time threshold: %s", timeThresholdMin);
    this.timeThresholdMin = timeThresholdMin;
  }

  /**
   * @return if true, output recommendations too
   */
  public boolean isRecommend() {
    return recommend;
  }

  public void setRecommend(boolean recommend) {
    this.recommend = recommend;
  }

  /**
   * @return if true, output item-item similarity too
   */
  public boolean isMakeItemSimilarity() {
    return makeItemSimilarity;
  }

  public void setMakeItemSimilarity(boolean makeItemSimilarity) {
    this.makeItemSimilarity = makeItemSimilarity;
  }

  /**
   * @return if true, make clusters of users and items
   */
  public boolean isCluster() {
    return cluster;
  }

  public void setCluster(boolean cluster) {
    this.cluster = cluster;
  }

  /**
   * @return if true, force a generation run at startup
   */
  public boolean isForceRun() {
    return forceRun;
  }

  public void setForceRun(boolean forceRun) {
    this.forceRun = forceRun;
  }

}
