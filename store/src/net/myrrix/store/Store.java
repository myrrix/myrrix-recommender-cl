/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.store;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.logging.Level;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.common.ClassUtils;

/**
 * Abstract interface to backing store -- for now, either HDFS or Amazon S3. This allows the Hadoop-compatible
 * and AWS-compatible binaries to write most of their code in terms of abstract operations and only specialize
 * in portions that touch infrastructure. Instead of paths and files, the file operations are written in terms
 * of "keys".
 *
 * @author Sean Owen
 * @since 1.0
 */
public abstract class Store implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(Store.class);

  static final int MAX_RETRIES = 3;

  private static Store instance = null;

  /**
   * @return an implementation appropriate to the current environment
   */
  public static Store get() {
    // Does not need to be thread-safe, several copies are OK
    if (instance == null) {
      instance = ClassUtils.loadInstanceOf("net.myrrix.store.DelegateStore", Store.class);
    }
    return instance;
  }
  
  protected Store() {
    // Suppress annoying messages from HTTP client on retry
    java.util.logging.Logger logger = 
        java.util.logging.Logger.getLogger("org.apache.http.impl.client.DefaultRequestDirector");
    logger.setLevel(Level.WARNING);
  }

  /**
   * Detects if a file or directory exists in the remote file system.
   *
   * @param key file to test
   * @param isFile if true, test for a file, otherwise for a directory
   * @return {@code true} iff the file exists
   */
  public abstract boolean exists(String key, boolean isFile) throws IOException;

  /**
   * Gets the size in bytes of a remote file.
   *
   * @param key file to test
   * @return size of file in bytes
   * @throws java.io.FileNotFoundException if there is no file at the key
   */
  public abstract long getSize(String key) throws IOException;

  /**
   * @param key text file to read
   * @return a character stream delivering the file's contents
   * @throws IOException if an error occurs, like the file doesn't exist
   */
  public abstract BufferedReader streamFrom(String key) throws IOException;

  /**
   * @param key location of file to download from distibuted storage
   * @param file local {@link File} to store data into
   * @throws IOException if an error occurs while downloading
   */
  public abstract void download(String key, File file) throws IOException;

  /**
   * Uploads a local file to a remote file.
   *
   * @param key file to write to
   * @param file file bytes to upload
   * @param overwrite if true, overwrite the existing file data if exists already
   * @throws IOException if the data can't be written, or file exists and overwrite is false
   */
  public final void upload(String key, File file, boolean overwrite) throws IOException {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(file);
    if (!overwrite && exists(key, true)) {
      throw new IOException(key + " already exists");
    }
    int failures = 0;
    while (true) {
      try {
        doUpload(key, file, overwrite);
        break;
      } catch (IOException ioe) {
        if (++failures >= MAX_RETRIES) {
          throw ioe;
        }
        log.warn("Upload failed ({}); retrying", ioe.getMessage());
      }
    }
  }

  /**
   * Worker method that uploads a local file to a remote file.
   *
   * @param key file to write to
   * @param file file bytes to upload
   * @param overwrite if true, overwrite the existing file data if exists already   
   * @throws IOException if the data can't be written
   */
  protected abstract void doUpload(String key, File file, boolean overwrite) throws IOException;

  /**
   * Creates a 0-length file.
   *
   * @param key file to create
   */
  public abstract void touch(String key) throws IOException;

  /**
   * Makes a directory. If the file system doesn't have an idea of directories, it makes a 0-length file.
   *
   * @param key directory to create
   */
  public abstract void mkdir(String key) throws IOException;

  /**
   * Moves a remote file to a new remote location.
   *
   * @param from current file location
   * @param to new location
   */
  public abstract void move(String from, String to) throws IOException;
  
  /**
   * Copy a remote file to a new remote location.
   *
   * @param from current file location
   * @param to new location
   */
  public abstract void copy(String from, String to) throws IOException;

  /**
   * Deletes the file at the given location.
   *
   * @param key file to delete
   */
  public abstract void delete(String key) throws IOException;

  /**
   * Recursively deletes a file/directory. If the file system does not have a notion of directories, this deletes
   * all keys that begin with the given prefix.
   *
   * @param keyPrefix file/directory ("prefix") to delete
   */
  public final void recursiveDelete(String keyPrefix) throws IOException {
    recursiveDelete(keyPrefix, null);
  }

  /**
   * Recursively deletes a file/directory. If the file system does not have a notion of directories, this deletes
   * all keys that begin with the given prefix. An optional filter can exclude deletion of some files.
   *
   * @param keyPrefix file/directory ("prefix") to delete
   * @param except if not {@code null}, matching files will not be deleted
   */
  public abstract void recursiveDelete(String keyPrefix, FilenameFilter except) throws IOException;

  /**
   * Lists contents of a directory. For file systems without a notion of directory, this lists prefixes that
   * have the same prefix as the given prefix, but excludes "directories" (keys with same prefix, but followed
   * by more path elements). Results are returned in lexicographically sorted order.
   *
   * @param prefix directory to list
   * @param files if true, only list files, not directories
   * @return list of keys representing directory contents
   */
  public abstract List<String> list(String prefix, boolean files) throws IOException;

  /**
   * @param key file to test
   * @return last-modified time of file, in milliseconds since the epoch
   * @throws java.io.FileNotFoundException if the file does not exist
   */
  public abstract long getLastModified(String key) throws IOException;

  /**
   * @return a URI where a human-readable user interface to the backing store is available
   */
  public abstract URI getManagementURI();

  @Override
  public abstract void close();

}
