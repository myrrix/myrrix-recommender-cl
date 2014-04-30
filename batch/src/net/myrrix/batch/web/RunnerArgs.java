/*
 * Copyright Myrrix Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.myrrix.batch.web;

import java.io.File;

import com.lexicalscope.jewel.cli.Option;

/**
 * Arguments for {@link Runner}.
 *
 * @author Sean Owen
 * @since 1.0
 */
public interface RunnerArgs {

  @Option(description = "Bucket storing data to access")
  String getBucket();

  @Option(description = "Instance ID to access")
  String getInstanceID();

  @Option(defaultValue = "1000", description = "New data in MB that will trigger a generation")
  String getData();

  @Option(defaultValue = "1440", description = "Time in minutes that will trigger a generation")
  String getTime();

  @Option(description = "If set, also compute recommendations")
  boolean isRecommend();

  @Option(description = "If set, also compute item-item similarity")
  boolean isItemSimilarity();

  @Option(description = "If set, also compute user and item clusters")
  boolean isCluster();

  @Option(description = "If set, force one generation run at startup")
  boolean isForceRun();

  @Option(defaultValue = "8080", description = "HTTP port number")
  int getPort();

  @Option(defaultValue = "8443", description = "HTTPS port number")
  int getSecurePort();

  @Option(defaultToNull = true, description = "User name needed to authenticate to this instance")
  String getUserName();

  @Option(defaultToNull = true, description = "Password to authenticate to this instance")
  String getPassword();

  @Option(description = "User name and password only apply to admin and console resources")
  boolean isConsoleOnlyPassword();

  @Option(defaultToNull = true, description = "Test SSL certificate keystore to accept")
  File getKeystoreFile();

  @Option(defaultToNull = true, description = "Password for keystoreFile")
  String getKeystorePassword();

  @Option(helpRequest = true)
  boolean getHelp();

}
