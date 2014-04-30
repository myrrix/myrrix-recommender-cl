/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.web;

import java.io.File;
import java.io.Serializable;

import com.google.common.base.Preconditions;

import net.myrrix.batch.PeriodicRunnerConfig;

/**
 * Encapsulates configuration for {@link Runner}.
 *
 * @author Sean Owen
 * @since 1.0
 */
public final class RunnerConfiguration implements Serializable {

  public static final int NO_THRESHOLD = Integer.MIN_VALUE;
  public static final int DEFAULT_PORT = 8080;
  public static final int DEFAULT_SECURE_PORT = 8443;

  private final PeriodicRunnerConfig periodicRunnerConfig;
  private int port;
  private int securePort;
  private File keystoreFile;
  private String keystorePassword;
  private File localInputDir;
  private String userName;
  private String password;
  private boolean consoleOnlyPassword;

  public RunnerConfiguration() {
    this.periodicRunnerConfig = new PeriodicRunnerConfig();
    this.port = DEFAULT_PORT;
    this.securePort = DEFAULT_SECURE_PORT;
  }

  public PeriodicRunnerConfig getPeriodicRunnerConfig() {
    return periodicRunnerConfig;
  }

  /**
   * @return port on which the Serving Layer listens for non-secure HTTP connections
   */
  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    Preconditions.checkArgument(port >= 0, "port must be nonnegative: %s", port);
    this.port = port;
  }

  /**
   * @return port on which the Serving Layer listens for secure HTTPS connections, if applicable
   */
  public int getSecurePort() {
    return securePort;
  }

  public void setSecurePort(int securePort) {
    Preconditions.checkArgument(securePort >= 0, "securePort must be nonnegative: %s", securePort);
    this.securePort = securePort;
  }

  /**
   * @return keystore file that contains the SSL keys used for HTTPS connections, if any; if not set
   *  the instance will not listen for HTTPS connections
   */
  public File getKeystoreFile() {
    return keystoreFile;
  }

  public void setKeystoreFile(File keystoreFile) {
    this.keystoreFile = keystoreFile;
  }

  /**
   * @return password for {@link #getKeystoreFile()}, if applicable
   */
  public String getKeystorePassword() {
    return keystorePassword;
  }

  public void setKeystorePassword(String keystorePassword) {
    this.keystorePassword = keystorePassword;
  }

  public File getLocalInputDir() {
    return localInputDir;
  }

  public void setLocalInputDir(File localInputDir) {
    this.localInputDir = localInputDir;
  }

  /**
   * @return user name which must be supplied to access the Serving Layer with HTTP DIGEST authentication,
   *  if applicable; none is required if this is not specified
   */
  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  /**
   * @return password which must be supplied to access the Serving Layer with HTTP DIGEST authentication,
   *  if applicable; none is required if this is not specified
   */
  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  /**
   * @return true if username and password only apply to admin / console resources
   */
  public boolean isConsoleOnlyPassword() {
    return consoleOnlyPassword;
  }

  public void setConsoleOnlyPassword(boolean consoleOnlyPassword) {
    this.consoleOnlyPassword = consoleOnlyPassword;
  }

}
