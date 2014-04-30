/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.store.Namespaces;
import net.myrrix.store.Store;

final class DeployRemoteJarFile {

  private static final Logger log = LoggerFactory.getLogger(DeployRemoteJarFile.class);

  private static final String[] EXCLUDE_NAMES = {
      // Exclude MANIFEST.MF since it specifies a Main-Class which we don't want (on EMR)
      "MANIFEST.MF", 
      "META-INF/maven/",
      // And exclude some SLF4J stuff to avoid incompatibility
      "org/slf4j/impl/",
      // And exclude other unneeded code for quick upload
      "org/apache/catalina/", "org/apache/coyote/", "org/apache/el/", "org/apache/jasper/", "org/apache/juli/",
      "org/apache/naming/",
      "org/apache/tomcat/",
      "javax/annotation/", "javax/el/", "javax/ejb/", "javax/servlet/", "javax/persistence/",
      "com/lexicalscope/",
      // Hadoop itself stuff
      "org/apache/hadoop/",
      "org/apache/commons/logging/",  
      "org/apache/commons/cli/",      
      "org/apache/commons/codec/",
      "org/apache/commons/configuration/",
      "org/apache/commons/lang/",
      "org/codehaus/jackson/",
  };

  private DeployRemoteJarFile() {
  }

  static void maybeUploadMyrrixJar(String instanceID) throws IOException {

    File jarFile = getJarFileForClass();
    long localLastModified = jarFile.lastModified();

    long remoteLastModified;

    String myrrixJarLocation = Namespaces.getMyrrixJarPrefix(instanceID);
    Store store = Store.get();
    if (store.exists(myrrixJarLocation, true)) {
      remoteLastModified = store.getLastModified(myrrixJarLocation);
    } else {
      remoteLastModified = 0L;
    }

    // Add slush to time check since we'll upload a modified version whose timestamp may be slightly
    // later than the original down here.
    if (localLastModified > remoteLastModified + TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES)) {
      File newJarFile = copyAndRemoveSensitive(jarFile);
      log.info("Uploading {} to {}", newJarFile, myrrixJarLocation);
      try {
        store.upload(myrrixJarLocation, newJarFile, true);
      } finally {
        if (!newJarFile.delete()) {
          log.info("Could not delete {}", newJarFile);
        }
      }
    } else {
      log.info("Remote JAR file appears to be up to date");
    }

  }

  private static File getJarFileForClass() {
    Class<?> c = DeployRemoteJarFile.class;
    ClassLoader cl = c.getClassLoader();
    String classResourceName = c.getName().replace(".", "/") + ".class";
    URL classResource = cl.getResource(classResourceName);
    if (classResource == null) {
      throw new IllegalStateException("Resource not found: " + classResourceName);
    }
    String pathToClass = classResource.getPath();
    int jarSeparator = pathToClass.lastIndexOf('!');
    Preconditions.checkState(pathToClass.startsWith("file:") && jarSeparator >= 0,
                             "Unsupported path to class: %s", pathToClass);
    String jarPath = pathToClass.substring("file:".length(), jarSeparator);
    return new File(jarPath);
  }

  private static File copyAndRemoveSensitive(File jarFile) throws IOException {
    File newJarFile = File.createTempFile(jarFile.getName() + '-', ".jar");
    newJarFile.deleteOnExit();

    boolean foundSensitive = false;

    Closer closer = Closer.create();
    try {
      ZipInputStream in = closer.register(new ZipInputStream(new FileInputStream(jarFile)));      
      ZipOutputStream out = closer.register(new ZipOutputStream(new FileOutputStream(newJarFile)));
      ZipEntry entry;
      while ((entry = in.getNextEntry()) != null) {
        String name = entry.getName();
        boolean excluded = false;
        for (CharSequence excludeName : EXCLUDE_NAMES) {
          if (name.contains(excludeName)) {
            excluded = true;
            break;
          }
        }
        if (excluded) {
          foundSensitive = true;
        } else {
          out.putNextEntry(entry);
          ByteStreams.copy(in, out);
        }
      }
    } finally {
      closer.close();
    }

    if (!foundSensitive) {
      log.warn("Didn't find files to remove in {}", jarFile);
      if (!newJarFile.delete()) {
        log.info("Could not delete {}", newJarFile);
      }
      return jarFile;
    }

    return newJarFile;
  }

}
