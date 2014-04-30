/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.store;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;
import java.util.zip.ZipInputStream;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation specialized for HDFS.
 * 
 * @author Sean Owen
 * @since 1.0
 */
public final class DelegateStore extends Store {

  private static final Logger log = LoggerFactory.getLogger(DelegateStore.class);

  private static final Pattern FS_DEFAULT_NAME_PATTERN = Pattern.compile("hdfs://([^:]+):[0-9]+");
  private static final Pattern DFS_HTTP_ADDRESS_PATTERN = Pattern.compile(":([0-9]+)");

  private final FileSystem fs;

  public DelegateStore() throws IOException {
    fs = FileSystem.get(URI.create(Namespaces.get().getBucketPath()), MyrrixConfiguration.get());
  }

  @Override
  public boolean exists(String key, boolean isFile) throws IOException {
    Path path = Namespaces.toPath(key);
    return fs.exists(path) && (fs.isFile(path) == isFile);
  }

  @Override
  public long getSize(String key) throws IOException {
    Path path = Namespaces.toPath(key);
    return fs.getFileStatus(path).getLen();
  }

  @Override
  public BufferedReader streamFrom(String key) throws IOException {
    InputStream in = fs.open(Namespaces.toPath(key));
    if (key.endsWith(".gz")) {
      in = new GZIPInputStream(in);
    } else if (key.endsWith(".zip")) {
      in = new ZipInputStream(in);
    } else if (key.endsWith(".deflate")) {
      in = new InflaterInputStream(in);
    } else if (key.endsWith(".bz2") || key.endsWith(".bzip2")) {
      in = new BZip2CompressorInputStream(in);
    }
    return new BufferedReader(new InputStreamReader(in, Charsets.UTF_8), 1 << 20); // ~1MB
  }

  private void makeParentDirs(Path path) throws IOException {
    Path parent = path.getParent();
    boolean success;
    try {
      success = fs.mkdirs(parent);
    } catch (AccessControlException ace) {
      log.error("Permissions problem; is {} writable in HDFS?", parent);
      throw ace;
    }
    if (!success) {
      throw new IOException("Can't make " + parent);
    }
  }

  @Override
  public void download(String key, File file) throws IOException {
    Path path = Namespaces.toPath(key);
    Path filePath = new Path(file.getAbsolutePath());
    fs.copyToLocalFile(true, path, filePath);
  }

  @Override
  protected void doUpload(String key, File file, boolean overwrite) throws IOException {
    Path path = Namespaces.toPath(key);
    makeParentDirs(path);
    Path filePath = new Path(file.getAbsolutePath());
    try { 
      fs.copyFromLocalFile(false, overwrite, filePath, path);
    } catch (AccessControlException ace) {
      log.error("Permissions problem; is {} writable in HDFS?", path);
      throw ace;
    }
    if (!fs.exists(path)) {
      throw new IOException("Couldn't upload " + filePath + " to " + path);
    }
  }

  @Override
  public void touch(String key) throws IOException {
    Path path = Namespaces.toPath(key);
    makeParentDirs(path);
    boolean success;
    try {
      success = fs.createNewFile(path);
    } catch (AccessControlException ace) {
      log.error("Permissions problem; is {} writable in HDFS?", path);
      throw ace;
    }
    if (!success) {
      throw new IOException("Can't create " + path);
    }
  }

  @Override
  public void mkdir(String key) throws IOException {
    Path path = Namespaces.toPath(key);
    boolean success;
    try {
      success = fs.mkdirs(path);
    } catch (AccessControlException ace) {
      log.error("Permissions problem; is {} writable in HDFS?", path);
      throw ace;
    }
    if (!success) {
      throw new IOException("Can't mkdirs for " + path);
    }
  }
  
  @Override
  public void move(String from, String to) throws IOException {
    Path fromPath = Namespaces.toPath(from);
    Path toPath = Namespaces.toPath(to);
    try {
      fs.rename(fromPath, toPath);
    } catch (AccessControlException ace) {
      log.error("Permissions problem; is {} readable and {} writable in HDFS?", fromPath, toPath);
      throw ace;
    }
  }
  
  @Override
  public void copy(String from, String to) throws IOException {
    Path fromPath = Namespaces.toPath(from);
    Path toPath = Namespaces.toPath(to);
    try {
      FileUtil.copy(fs, fromPath, fs, toPath, false, true, fs.getConf());
    } catch (AccessControlException ace) {
      log.error("Permissions problem; is {} readable and {} writable in HDFS?", fromPath, toPath);
      throw ace;
    }
  }

  @Override
  public void delete(String key) throws IOException {
    Path path = Namespaces.toPath(key);
    if (!fs.isFile(path)) {
      throw new IOException("Not a file: " + path);
    }
    boolean success;
    try {
      success = fs.delete(path, false);
    } catch (AccessControlException ace) {
      log.error("Permissions problem; is {} writable in HDFS?", path);
      throw ace;
    }
    if (!success) {
      throw new IOException("Can't delete " + path);
    }
  }

  @Override
  public void recursiveDelete(String keyPrefix, FilenameFilter except) throws IOException {
    if (except != null) {
      throw new UnsupportedOperationException("Can't implement 'except'");
    }
    Path path = Namespaces.toPath(keyPrefix);
    if (!fs.exists(path)) {
      log.info("Nothing to delete as {} does not exist", path);
      return;
    }

    boolean success;
    try {
      log.info("Deleting recursively: {}", path);
      success = fs.delete(path, true);
    } catch (AccessControlException ace) {
      log.error("Permissions problem; is {} writable in HDFS?", path);
      throw ace;
    }
    if (!success) {
      throw new IOException("Can't delete " + path);
    }
  }

  @Override
  public List<String> list(String prefix, boolean files) throws IOException {
    Path path = Namespaces.toPath(prefix);
    if (!fs.exists(path)) {
      return Collections.emptyList();
    }
    
    Preconditions.checkArgument(fs.getFileStatus(path).isDir(), "Not a directory: %s", path);
    FileStatus[] statuses = fs.listStatus(path, new FilesOrDirsPathFilter(fs, files));
    String prefixString = Namespaces.get().getBucketPath();

    List<String> result = Lists.newArrayListWithCapacity(statuses.length);
    for (FileStatus fileStatus : statuses) {
      String listPath = fileStatus.getPath().toString();
      Preconditions.checkState(listPath.startsWith(prefixString),
                               "%s doesn't start with %s", listPath, prefixString);
      if (!listPath.endsWith("_SUCCESS")) {
        listPath = listPath.substring(prefixString.length());
        if (fileStatus.isDir() && !listPath.endsWith("/")) {
          listPath += "/";
        }
        result.add(listPath);
      }
    }
    Collections.sort(result);
    return result;
  }

  @Override
  public long getLastModified(String key) throws IOException {
    Path path = Namespaces.toPath(key);
    FileStatus status = fs.getFileStatus(path);
    return status.getModificationTime();
  }

  @Override
  public URI getManagementURI() {

    Configuration config = MyrrixConfiguration.get();

    String fsDefaultName = config.get("fs.defaultFS");
    Preconditions.checkNotNull(fsDefaultName);
    Matcher fsDefaultNameMatcher = FS_DEFAULT_NAME_PATTERN.matcher(fsDefaultName);
    Preconditions.checkState(fsDefaultNameMatcher.find(), "Bad fs.defaultFS: {}", fsDefaultName);
    String host = fsDefaultNameMatcher.group(1);

    String dfsHttpAddress = config.get("dfs.http.address");
    Preconditions.checkNotNull(dfsHttpAddress);
    Matcher dfsHttpAddressMatcher = DFS_HTTP_ADDRESS_PATTERN.matcher(dfsHttpAddress);
    Preconditions.checkState(dfsHttpAddressMatcher.find(), "Bad dfs.http.address: {}", dfsHttpAddress);
    int port = Integer.parseInt(dfsHttpAddressMatcher.group(1));

    return URI.create("http://" + host + ':' + port);
  }

  @Override
  public void close() {
    log.info("Shutting down Store...");
    try {
      fs.close();
    } catch (IOException e) {
      log.warn("Failed to close FileSystem", e);
    }
  }

}
