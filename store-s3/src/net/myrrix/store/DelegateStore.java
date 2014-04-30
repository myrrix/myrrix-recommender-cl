/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.store;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;
import java.util.zip.ZipInputStream;

import com.amazonaws.AmazonClientException;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.math3.util.FastMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.common.parallel.ExecutorUtils;
import net.myrrix.common.io.IOUtils;
import net.myrrix.common.io.NullInputStream;

/**
 * An implementation specialized for Amazon S3.
 * 
 * @author Sean Owen
 * @since 1.0
 */
public final class DelegateStore extends Store {

  private static final Logger log = LoggerFactory.getLogger(DelegateStore.class);

  private static final URI MANAGEMENT_URL = URI.create("https://console.aws.amazon.com/s3/home");
  private static final Pattern PERIOD = Pattern.compile("\\.");
  private static final long PARALLEL_UPLOAD_SIZE = 4 * 5 * 1024 * 1024L; // Must be at least 2x 5MB minimum

  private final AmazonS3Client s3Client;
  private final ExecutorService executorService;

  public DelegateStore() {
    s3Client = new AmazonS3Client(S3Utils.getAWSCredentialsProvider(), S3Utils.buildClientConfiguration());
    executorService = Executors.newFixedThreadPool(4, 
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("DelegateStore-%d").build());
  }

  @Override
  public boolean exists(String key, boolean isFile) throws IOException {
    if (isFile) {
      Preconditions.checkArgument(key.charAt(key.length() - 1) != '/',
                                  "File should not end in %s: %s", '/', key);
    } else {
      Preconditions.checkArgument(key.charAt(key.length() - 1) == '/',
                                  "Dir should end in %s: %s", '/', key);
    }
    ObjectListing listing;
    try {
      listing = s3Client.listObjects(Namespaces.get().getBucket(), key);
    } catch (AmazonClientException ace) {
      throw new IOException(ace);
    }
    if (listing != null) {
      for (S3ObjectSummary s3os : listing.getObjectSummaries()) {
        String object = s3os.getKey();
        if (object.equals(key) || (!isFile && object.startsWith(key))) {
          // Counts if the keys match exactly, or, the match continues with a subdir path in which
          // case this must be a directory
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public long getSize(String key) throws IOException {
    ObjectMetadata metadata;
    try {
      metadata = s3Client.getObjectMetadata(Namespaces.get().getBucket(), key);
    } catch (AmazonClientException ace) {
      throw new IOException(ace);
    }
    if (metadata == null) {
      throw new FileNotFoundException(key);
    }
    return metadata.getContentLength();
  }

  @Override
  public BufferedReader streamFrom(String key) throws IOException {

    S3Object s3Object;
    try {
      s3Object = s3Client.getObject(Namespaces.get().getBucket(), key);
    } catch (AmazonClientException ace) {
      throw new IOException(ace);
    }

    String[] contentTypeEncoding = guessContentTypeEncodingFromExtension(key);
    String encoding = contentTypeEncoding[1];

    InputStream in = s3Object.getObjectContent();
    try {
      if ("gzip".equals(encoding)) {
        in = new GZIPInputStream(in);
      } else if ("zip".equals(encoding)) {
        in = new ZipInputStream(in);
      } else if ("deflate".equals(encoding)) {
        in = new InflaterInputStream(in);
      } else if ("bzip2".equals(encoding)) {
        in = new BZip2CompressorInputStream(in);
      } else {
        ObjectMetadata metadata = s3Object.getObjectMetadata();
        if (metadata != null) {
          String contentEncoding = metadata.getContentEncoding();
          if ("gzip".equals(contentEncoding)) {
            in = new GZIPInputStream(in);
          } else if ("zip".equals(contentEncoding)) {
            in = new ZipInputStream(in);
          } else if ("deflate".equals(contentEncoding)) {
            in = new InflaterInputStream(in);
          } else if ("bzip2".equals(contentEncoding)) {
            in = new BZip2CompressorInputStream(in);
          }
        }
      }
    } catch (IOException ioe) {
      // Be extra sure this doesn't like in case of an error
      in.close();
      throw ioe;
    }

    return new BufferedReader(new InputStreamReader(in, Charsets.UTF_8), 1 << 20); // ~1MB
  }

  @Override
  public void download(String key, File file) throws IOException {
    S3Object s3Object;
    try {
      s3Object = s3Client.getObject(Namespaces.get().getBucket(), key);
    } catch (AmazonClientException ace) {
      throw new IOException(ace);
    }
    file.delete();
    InputStream in = s3Object.getObjectContent();
    try {
      IOUtils.copyStreamToFile(in, file);
    } finally {
      in.close();
    }
  }

  @Override
  public void touch(String key) throws IOException {
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(0);
    try {
      s3Client.putObject(Namespaces.get().getBucket(), key, NullInputStream.INSTANCE, metadata);
    } catch (AmazonClientException ace) {
      throw new IOException(ace);
    }
  }

  @Override
  protected void doUpload(String key, File file, boolean overwrite) throws IOException {
    String bucket = Namespaces.get().getBucket();
    String[] contentTypeEncoding = guessContentTypeEncodingFromExtension(file.getName()); 
    ObjectMetadata metadata = new ObjectMetadata();
    if (contentTypeEncoding[0] != null) {
      metadata.setContentType(contentTypeEncoding[0]);
    }
    if (contentTypeEncoding[1] != null) {
      metadata.setContentEncoding(contentTypeEncoding[1]);
    }
    long contentLength = file.length();
    try {
      if (contentLength > PARALLEL_UPLOAD_SIZE) {
        doParallelUpload(bucket, key, file, metadata);          
      } else {
        doSerialUpload(bucket, key, file, metadata);
      }
    } catch (AmazonClientException ace) {
      throw new IOException(ace);
    }
  }
  
  private void doParallelUpload(String bucket, String key, File file, ObjectMetadata metadata) throws IOException {

    long contentLength = file.length();
    long numParts = (contentLength + PARALLEL_UPLOAD_SIZE - 1) / PARALLEL_UPLOAD_SIZE;
    long partSize = (contentLength / numParts) + 1;
    
    log.info("Uploading {} in {} parts with part size {}", file, numParts, partSize);    
    
    final List<PartETag> partETags = Lists.newArrayList();    
    InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucket, key, metadata);
    InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
    String uploadID = initResponse.getUploadId();

    try {      
      long filePosition = 0;
      int partNumber = 1;
      Collection<Future<?>> futures = Lists.newArrayList();
      while (filePosition < contentLength) {
        
        final UploadPartRequest uploadRequest = new UploadPartRequest()
            .withBucketName(bucket)
            .withKey(key)
            .withUploadId(uploadID)
            .withPartNumber(partNumber)
            .withFileOffset(filePosition)
            .withFile(file)
            .withPartSize(FastMath.min(partSize, contentLength - filePosition));
        
        futures.add(executorService.submit(new Callable<Object>() {
          @Override
          public Object call() {
            String theKey = uploadRequest.getKey();
            int thePartNumber = uploadRequest.getPartNumber();
            log.info("Starting {} part {}", theKey, thePartNumber);
            int failures = 0;
            UploadPartResult uploadPartResult;
            while (true) {
              try {
                uploadPartResult = s3Client.uploadPart(uploadRequest);
                break;
              } catch (AmazonClientException ace) {
                if (++failures >= MAX_RETRIES) {
                  throw ace;
                }
                log.warn("Upload {} part {} failed ({}); retrying", theKey, thePartNumber, ace.getMessage());
              }
            }
            partETags.add(uploadPartResult.getPartETag());
            log.info("Finished {} part {}", theKey, thePartNumber);            
            return null;
          }
        }));
        
        filePosition += partSize;
        partNumber++;
      }
      
      for (Future<?> future : futures) {
        try {
          future.get();
        } catch (InterruptedException e) {
          throw new IOException(e);
        } catch (ExecutionException e) {
          throw new IOException(e.getCause());
        }
      }
      
      CompleteMultipartUploadRequest completeRequest = 
          new CompleteMultipartUploadRequest(bucket, key, uploadID, partETags);
      s3Client.completeMultipartUpload(completeRequest);
      
    } catch (AmazonClientException ace) {
      AbortMultipartUploadRequest abortRequest = new AbortMultipartUploadRequest(bucket, key, uploadID);
      s3Client.abortMultipartUpload(abortRequest);
      throw ace;
    }
  }
  
  private void doSerialUpload(String bucket, String key, File file, ObjectMetadata metadata) throws IOException {
    log.info("Uploading {} in single thread", file);
    long contentLength = file.length();
    metadata.setContentLength(contentLength);
    InputStream in = new FileInputStream(file);
    try {
      s3Client.putObject(bucket, key, in, metadata);
    } finally {
      in.close();
    }
  }

  private static String[] guessContentTypeEncodingFromExtension(CharSequence fileName) {
    String[] result = { null, null };
    String[] tokens = PERIOD.split(fileName);
    String suffix = tokens[tokens.length - 1];
    // First take off compression
    if ("gz".equals(suffix) || "gzip".equals(suffix) || 
        "bz2".equals(suffix) || "bzip2".equals(suffix) || 
        "zip".equals(suffix) || "deflate".equals(suffix)) {
      if ("gz".equals(suffix)) {
        result[1] = "gzip"; // Special case gz -> gzip
      } else if ("bz2".equals(suffix)) {
        result[1] = "bzip2"; // Special case bz2 -> bzip2
      } else {
        result[1] = suffix;
      }
      if (tokens.length >= 2) {
        suffix = tokens[tokens.length - 2];
      }
    }
    if ("txt".equals(suffix) || "csv".equals(suffix)) {
      result[0] = "text/plain";
    }
    return result;
  }

  @Override
  public void mkdir(String key) throws IOException {
    touch(key);
  }
  
  @Override
  public void move(String from, String to) throws IOException {
    String bucket = Namespaces.get().getBucket();
    try {
      copy(from, to);
      s3Client.deleteObject(bucket, from);
    } catch (AmazonClientException ace) {
      throw new IOException(ace);
    }
  }
  
  @Override
  public void copy(String from, String to) throws IOException {
    String bucket = Namespaces.get().getBucket();
    try {
      s3Client.copyObject(bucket, from, bucket, to);
    } catch (AmazonClientException ace) {
      throw new IOException(ace);
    }
  }

  @Override
  public void delete(String key) throws IOException {
    try {
      s3Client.deleteObject(Namespaces.get().getBucket(), key);
    } catch (AmazonClientException ace) {
      throw new IOException(ace);
    }
  }

  @Override
  public void recursiveDelete(String keyPrefix, FilenameFilter except) throws IOException {

    if (keyPrefix.charAt(keyPrefix.length() - 1) == '/') {
      // Need to delete the dir too, and to list it, can't specify its trailing slash
      keyPrefix = keyPrefix.substring(0, keyPrefix.length() - 1);
    }

    boolean truncated = true;
    String marker = null;

    String bucket = Namespaces.get().getBucket();
    while (truncated) {

      ListObjectsRequest listRequest = new ListObjectsRequest(bucket, keyPrefix, marker, null, null);
      ObjectListing listing;
      try {
        listing = s3Client.listObjects(listRequest);
        Collection<S3ObjectSummary> summaries = listing.getObjectSummaries();
        if (summaries.isEmpty()) {
          return;
        }

        List<DeleteObjectsRequest.KeyVersion> keysToDelete = Lists.newArrayListWithCapacity(summaries.size());
        for (S3ObjectSummary summary : summaries) {
          String key = summary.getKey();
          if (except == null || !except.accept(null, key)) {
            keysToDelete.add(new DeleteObjectsRequest.KeyVersion(key));
          }
        }

        DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucket);
        deleteObjectsRequest.setKeys(keysToDelete);
        s3Client.deleteObjects(deleteObjectsRequest);
      } catch (AmazonClientException ace) {
        throw new IOException(ace);
      }

      truncated = listing.isTruncated();
      marker = listing.getNextMarker();
    }
  }

  @Override
  public List<String> list(String prefix, boolean files) throws IOException {
    boolean truncated = true;
    String marker = null;
    List<String> result = Lists.newArrayList();
    while (truncated) {
      if (marker != null) {
        log.info("Next page after {}", marker);
      }
      ListObjectsRequest listRequest = new ListObjectsRequest(Namespaces.get().getBucket(),
                                                              prefix,
                                                              marker,
                                                              "/",
                                                              null);
      ObjectListing listing;
      try {
        listing = s3Client.listObjects(listRequest);
      } catch (AmazonClientException ace) {
        throw new IOException(ace);
      }
      truncated = listing.isTruncated();
      marker = listing.getNextMarker();
      if (!truncated) {
        if (files) {
          for (S3ObjectSummary summary : listing.getObjectSummaries()) {
            String key = summary.getKey();
            if (!key.endsWith("_SUCCESS")) {
              result.add(key);
            }
          }
        } else {
          for (String key : listing.getCommonPrefixes()) {
            if (!key.endsWith("_SUCCESS")) {
              result.add(key);
            }
          }
        }
      }
    }
    Collections.sort(result);
    return result;
  }

  @Override
  public long getLastModified(String key) throws IOException {
    ObjectMetadata metadata;
    try {
      metadata = s3Client.getObjectMetadata(Namespaces.get().getBucket(), key);
    } catch (AmazonClientException ace) {
      throw new IOException(ace);
    }
    if (metadata == null) {
      throw new FileNotFoundException(key);
    }
    return metadata.getLastModified().getTime();
  }

  @Override
  public URI getManagementURI() {
    return MANAGEMENT_URL;
  }

  @Override
  public void close() {
    ExecutorUtils.shutdownNowAndAwait(executorService);
    log.info("Shutting down Store...");
    s3Client.shutdown();
    // Weird we must do this directly:
    IdleConnectionReaper.shutdown();
  }

}
