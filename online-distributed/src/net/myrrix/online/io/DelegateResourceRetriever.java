/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.online.io;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.store.Namespaces;
import net.myrrix.store.Store;

/**
 * An implementation that can handle local http:// URLs (and others which {@link URL} can handle natively),
 * and anything else that {@link Store} can load as a key.
 *
 * @author Sean Owen
 * @since 1.0
 */
public final class DelegateResourceRetriever implements ResourceRetriever {

  private static final Logger log = LoggerFactory.getLogger(DelegateResourceRetriever.class);

  @Override
  public void init(String bucket) {
    Namespaces.setGlobalBucket(bucket);
  }

  @Override
  public File getRescorerJar(String instanceID) throws IOException {
    return getResourceAsFile(Namespaces.getRescorerJarPrefix(instanceID));
  }
  
  @Override
  public File getClientThreadJar(String instanceID) throws IOException {
    return getResourceAsFile(Namespaces.getClientThreadJarPrefix(instanceID));    
  }  

  @Override
  public File getKeystoreFile(String instanceID) throws IOException {
    return getResourceAsFile(Namespaces.getKeystoreFilePrefix(instanceID));
  }

  private static File getResourceAsFile(String resource) throws IOException {
    Preconditions.checkNotNull(resource);

    String suffix;
    int dot = resource.lastIndexOf('.');
    if (dot >= 0) {
      suffix = resource.substring(dot);
    } else {
      suffix = ".tmp";
    }
    File tempFile = File.createTempFile("myrrix-", suffix);
    tempFile.deleteOnExit();

    log.info("Downloading {} to {}", resource, tempFile);
    Store.get().download(resource, tempFile);
    return tempFile;
  }

}
