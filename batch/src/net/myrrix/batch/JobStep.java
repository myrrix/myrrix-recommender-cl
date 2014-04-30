/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.batch.common.PartFilePathFilter;
import net.myrrix.common.log.MemoryHandler;
import net.myrrix.store.MyrrixConfiguration;
import net.myrrix.store.Namespaces;
import net.myrrix.store.Store;

/**
 * Abstract superclass of all "steps", which are one MapReduce in Amazon EMR.
 *
 * @author Sean Owen
 * @since 1.0
 */
public abstract class JobStep extends Configured implements Tool, HasState {

  private static final Logger log = LoggerFactory.getLogger(JobStep.class);
  private static final String DEFAULT_BASE_NUM_REDUCERS = "4";

  private JobStepConfig config;
  private Date startTime;
  private Date endTime;
  private Job job;

  protected final String getInstanceID() {
    return config.getInstanceID();
  }

  protected final long getGenerationID() {
    return config.getGenerationID();
  }

  protected final long getLastGenerationID() {
    return config.getLastGenerationID();
  }

  protected final int getIteration() {
    return config.getIteration();
  }

  /**
   * @return true iff this is a job which may be used to compute some function of the X (user-feature) or Y
   *  (item-feature) matrix, and this job is computing a function of X. Not used in all jobs.
   */
  public final boolean isComputingX() {
    return config.isComputingX();
  }

  @Override
  public final Collection<MyrrixStepState> getStepStates() throws IOException {
    String name = getCustomJobName();
    MyrrixStepStatus status = determineStatus();
    MyrrixStepState state = new MyrrixStepState(startTime, endTime, name, status);
    URI managementURL = determineManagementURI();
    state.setManagementURI(managementURL);
    float[] progresses = determineProgresses();
    if (progresses != null) {
      state.setMapProgress(progresses[1]);
      state.setReduceProgress(progresses[2]);
    } else {
      if (status == MyrrixStepStatus.COMPLETED) {
        // This completed, without ever running, which means Hadoop doesn't report state because
        // it was skipped. It's complete
        state.setMapProgress(1.0f);
        state.setReduceProgress(1.0f);
      }
    }
    return Collections.singletonList(state);
  }

  private MyrrixStepStatus determineStatus() throws IOException {
    if (job == null) {
      return MyrrixStepStatus.COMPLETED;
    }
    JobClient client = new JobClient(new JobConf(getConf()));
    try {
      JobID jobID = job.getJobID();
      if (jobID == null) {
        return MyrrixStepStatus.PENDING;
      }
      RunningJob runningJob =
          client.getJob(new org.apache.hadoop.mapred.JobID(jobID.getJtIdentifier(), jobID.getId()));
      if (runningJob == null) {
        return MyrrixStepStatus.PENDING;
      }
      int state = runningJob.getJobState();
      // Can't use switch since these are no longer compile-time constants in Hadoop 2.x
      if (state == JobStatus.PREP) {
        return MyrrixStepStatus.PENDING;
      }
      if (state == JobStatus.RUNNING) {
        return MyrrixStepStatus.RUNNING;
      }
      if (state == JobStatus.FAILED) {
        return MyrrixStepStatus.FAILED;
      }
      if (state == JobStatus.KILLED) {
        return MyrrixStepStatus.CANCELLED;
      }
      if (state == JobStatus.SUCCEEDED) {
        return MyrrixStepStatus.COMPLETED;
      }
      throw new IllegalArgumentException("Unknown Hadoop job state " + state);
    } finally {
      client.close();
    }
  }

  private URI determineManagementURI() throws IOException {
    if (job == null) {
      return null;
    }
    JobClient client = new JobClient(new JobConf(getConf()));
    try {
      JobID jobID = job.getJobID();
      if (jobID == null) {
        return null;
      }
      RunningJob runningJob =
          client.getJob(new org.apache.hadoop.mapred.JobID(jobID.getJtIdentifier(), jobID.getId()));
      if (runningJob == null) {
        return null;
      }
      return URI.create(runningJob.getTrackingURL());
    } finally {
      client.close();
    }
  }

  /**
   * @return three progress values, in [0,1], as a {@code float[]}, representing setup, mapper and reducer progress
   */
  private float[] determineProgresses() throws IOException {
    if (job == null) {
      return null;
    }
    JobClient client = new JobClient(new JobConf(getConf()));
    try {
      JobID jobID = job.getJobID();
      if (jobID == null) {
        return null;
      }
      RunningJob runningJob =
          client.getJob(new org.apache.hadoop.mapred.JobID(jobID.getJtIdentifier(), jobID.getId()));
      if (runningJob == null) {
        return null;
      }
      return new float[] { runningJob.setupProgress(), runningJob.mapProgress(), runningJob.reduceProgress() };
    } finally {
      client.close();
    }
  }

  @Override
  public final int run(String[] args) throws InterruptedException, IOException {

    MemoryHandler.setSensibleLogFormat();

    if (log.isInfoEnabled()) {
      log.info("args are {}", Arrays.toString(args));
    }

    config = JobStepConfig.fromArgsArray(args);
    Namespaces.setGlobalBucket(config.getBucket());      
    
    String name = getCustomJobName();
    log.info("Running {}", name);

    long start = System.currentTimeMillis();
    startTime = new Date(start);
    try {

      job = buildJob();
      if (job == null) {
        log.info("{} does not need to run", name);
      } else {
        log.info("Waiting for {} to complete", name);
        if (!job.waitForCompletion(true)) {
          throw new IOException("waitForCompletion returned false in " + name);
        }
        log.info("Finished {}", name);
        postRun();
      }

    } catch (ClassNotFoundException cnfe) {
      log.error("Unexpected error while running {}", name, cnfe);
      throw new IllegalStateException(cnfe);

    } catch (IOException ioe) {
      log.error("Unexpected error while running {}", name, ioe);
      killJob(job);
      throw ioe;

    } catch (InterruptedException ie) {
      log.warn("Interrupted {}", name);
      killJob(job);
      throw ie;

    } finally {
      long end = System.currentTimeMillis();
      endTime = new Date(end);
      log.info("Completed {} in {}s", this, (end - start) / 1000L);
    }

    return 0;
  }

  private static void killJob(Job job) {
    if (job == null) {
      return;
    }
    try {
      job.killJob();
    } catch (IOException ioe2) {
      log.warn("Ignoring unexpected exception while killing: {}", ioe2.toString());
    } catch (IllegalStateException ise) {
      log.info("Job never started: {}", ise.toString());
    } catch (Exception e) {
      // Really, intending to catch InterruptedException here, which is only thrown in Hadoop 2.x
      if (e instanceof InterruptedException) {
        log.warn("Interrupted while killing; continuing: {}", e.toString());
      } else {
        // Should be a RuntimeException. Shouldn't happen anyway.
        throw new IllegalStateException(e);
      }
    }
  }

  /**
   * Subclasses override this and typically just call
   * {@link #prepareJob(String, String, Class, Class, Class, Class, Class, Class)}
   * to make the {@link Job}.
   *
   * @return {@link Job} encapsulating the MapReduce to run for this step or null if there is nothing
   *  to run
   */
  protected Job buildJob() throws IOException {
    return null;
  }

  /**
   * Subclasses override this to perform any steps after the job has finished.
   */
  protected void postRun() {
    // do nothing
  }

  /**
   * Configures a {@link Job} suitable for executing on a Hadoop cluster. This method sets many default parameters,
   * and accepts key arguments as method parameters here for convenience. Subclasses typically override
   * {@link #buildJob()} to configure a basic object from this method, then further customize and return it.
   *
   * @param inputPathKey location where input files for the step are located
   * @param outputPathKey location where step output should be written
   * @param mapper {@link Mapper} class used in the step, or {@code null} if it will be configured afterwards
   * @param mapperKey {@link Writable} class of keys to {@link Mapper}
   * @param mapperValue {@link Writable} class of values to {@link Mapper}
   * @param reducer {@link Reducer} class used in the step, or {@code null} for no reduce
   * @param reducerKey {@link Writable} class of keys to {@link Reducer}
   * @param reducerValue {@link Writable} class of values to {@link Reducer}
   * @return {@link Job} suitable for running on Hadoop, or further configuration in {@link #buildJob()}
   * @throws IOException if the underlying storage system can't be accessed
   */
  protected final <M extends Mapper<?,?,MK,MV>,
                   MK extends Writable,
                   MV extends Writable,
                   R extends Reducer<MK,MV,RK,RV>,
                   RK extends Writable,
                   RV extends Writable> Job prepareJob(String inputPathKey,
                                                       String outputPathKey,
                                                       Class<M> mapper,
                                                       Class<MK> mapperKey,
                                                       Class<MV> mapperValue,
                                                       Class<R> reducer,
                                                       Class<RK> reducerKey,
                                                       Class<RV> reducerValue) throws IOException {

    Preconditions.checkArgument(inputPathKey == null || inputPathKey.endsWith("/"),
                                "%s should end with /", inputPathKey);
    Preconditions.checkArgument(outputPathKey == null || outputPathKey.endsWith("/"),
                                "%s should end with /", outputPathKey);

    String jobName = getCustomJobName();

    Store store = Store.get();
    if (store.exists(outputPathKey, false)) {
      log.info("Output path {} already exists", outputPathKey);
      if (store.exists(outputPathKey + "_SUCCESS", true)) {
        log.info("{} shows _SUCCESS present; skipping", jobName);
        return null;
      }
      log.info("{} seems to have stopped during an earlier run; deleting output at {} and recomputing",
               jobName, outputPathKey);
      store.recursiveDelete(outputPathKey);
    } else {
      log.info("Output path is clear for writing: {}", outputPathKey);
    }

    MyrrixConfiguration conf = MyrrixConfiguration.get(getConf());
    
    conf.setInstanceID(getInstanceID());
    conf.setGenerationID(getGenerationID());
    conf.setBucketName(Namespaces.get().getBucket());

    // Tuned for a bit faster sorting: see also setting in DelegateConfiguration!
    conf.setInt("io.file.buffer.size", 1 << 17); // ~131K
    //conf.setInt("io.bytes.per.checksum", 1 << 13); // ~8K
    conf.setInt("io.sort.factor", 32);
    conf.setInt("io.sort.mb", 200);

    conf.setBoolean("mapred.compress.map.output", true);

    conf.setBoolean("mapred.map.tasks.speculative.execution", true);
    conf.setBoolean("mapred.reduce.tasks.speculative.execution", true);
    // Does this have effect here?
    conf.setBoolean("mapreduce.tasktracker.outofband.heartbeat", true);

    // Hacky: need to not set this EMR because of a bug (?)
    if ("emr".equals(conf.get("myrrix.environment"))) {
      log.info("Avoiding BLOCK compression type due to S3 issue");
    } else {
      conf.set("mapred.output.compression.type", SequenceFile.CompressionType.BLOCK.toString());
    }
    
    boolean isHadoop2 = isHadoop2(conf);
    
    // See below
    if (!isHadoop2) {
      Path remoteJarPath = Namespaces.toPath(Namespaces.getMyrrixJarPrefix(getInstanceID()));    
      DistributedCache.addFileToClassPath(remoteJarPath, conf, remoteJarPath.getFileSystem(conf));
      // Temporary, to maintain compatibility with CDH4 / Hadoop 2, but see below
      conf.set("mapreduce.job.classpath.files", conf.get("mapred.job.classpath.files"));
      conf.set("mapreduce.job.cache.files", conf.get("mapred.cache.files"));
    }

    // Make sure to set any args to conf above this line!

    setConf(conf);

    Job job = new Job(conf);
    
    // See above
    if (isHadoop2) {
      // DistributedCache thing doesn't work on Hadoop 2 somehow... resort to setJarByClass()
      if (Reducer.class.equals(reducer)) {
        Preconditions.checkState(mapper != null && !Mapper.class.equals(mapper),
                                 "Can't figure out the user class jar file from mapper/reducer");
        job.setJarByClass(mapper);
      } else {
        job.setJarByClass(reducer);
      }
    }

    job.setInputFormatClass(SequenceFileInputFormat.class);

    if (inputPathKey != null) {
      Path dir = Namespaces.toPath(inputPathKey);
      FileSystem fs = dir.getFileSystem(conf);
      Preconditions.checkArgument(fs.exists(dir) && fs.getFileStatus(dir).isDir(), "Not a directory: %s", dir);
      FileStatus[] statuses = fs.listStatus(dir, PartFilePathFilter.INSTANCE);
      Preconditions.checkArgument(statuses != null && statuses.length > 0, "No input paths exist at %s", dir);
      FileInputFormat.setInputPaths(job, FileUtil.stat2Paths(statuses));
    }

    FileInputFormat.setMaxInputSplitSize(job, 1L << 30); // ~1073MB
    FileInputFormat.setMinInputSplitSize(job, 1L << 27); // ~134MB

    if (mapper != null) {
      job.setMapperClass(mapper);
    }
    job.setMapOutputKeyClass(mapperKey);
    job.setMapOutputValueClass(mapperValue);

    job.setPartitionerClass(HashPartitioner.class);

    job.setReducerClass(reducer);
    job.setOutputKeyClass(reducerKey);
    job.setOutputValueClass(reducerValue);

    job.setJobName(jobName);

    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    FileOutputFormat.setCompressOutput(job, true);  

    FileOutputFormat.setOutputPath(job, Namespaces.toPath(outputPathKey));

    // Let -Dmapred.reduce.tasks override the default
    int numReducers = Integer.parseInt(System.getProperty("mapred.reduce.tasks", DEFAULT_BASE_NUM_REDUCERS));
    log.info("Using {} reducers", numReducers);

    job.setNumReduceTasks(numReducers);

    int reducesPerWorker = conf.getInt("mapreduce.tasktracker.reduce.tasks.maximum", 0);
    if (reducesPerWorker <= 0) {
      reducesPerWorker = conf.getInt("mapred.tasktracker.reduce.tasks.maximum", 0);
    }
    if (reducesPerWorker <= 0) {
      reducesPerWorker = 1;
    }
    if (reducesPerWorker > 0) {
      log.info("Parallelism is {} and max reducers per worker is {}. Up to {} machines can be effectively used",
               numReducers, reducesPerWorker, (numReducers + reducesPerWorker - 1) / reducesPerWorker);
    }

    log.info("Starting with job configuration {}", job.getConfiguration());

    return job;
  }

  /**
   * Sets output to be text based, and use compression
   */
  protected static void setUseTextOutput(Job job) {
    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
  }
  
  private static boolean isHadoop2(Configuration conf) {
    String value = conf.get("hadoop.common.configuration.version");
    boolean result =  
        value != null && (value.startsWith("2.") || (value.startsWith("0.2") && !value.startsWith("0.20")));
    log.info("Hadoop 2.x-style distribution? {} (hadoop.common.configuration.version={})", result, value); 
    return result;
  }

  public static int run(Tool step, String[] args) throws IOException, InterruptedException, JobException {
    log.info("Running step {}", step.getClass().getSimpleName());
    try {
      return ToolRunner.run(MyrrixConfiguration.get(), step, args);
    } catch (IOException ioe) {
      throw ioe;
    } catch (InterruptedException ie) {
      throw ie;
    } catch (Exception e) {
      throw new JobException(e);
    }
  }

  protected final String defaultCustomJobName() {
    StringBuilder name = new StringBuilder(100);
    name.append("Myrrix-").append(getInstanceID());
    name.append('-').append(getGenerationID());
    name.append('-').append(getClass().getSimpleName());
    return name.toString();
  }

  protected String getCustomJobName() {
    return defaultCustomJobName();
  }

  @Override
  public final String toString() {
    return getClass().getSimpleName();
  }

}
