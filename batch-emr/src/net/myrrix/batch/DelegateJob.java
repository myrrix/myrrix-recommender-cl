/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeSpotPriceHistoryRequest;
import com.amazonaws.services.ec2.model.DescribeSpotPriceHistoryResult;
import com.amazonaws.services.ec2.model.SpotPrice;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceRoleType;
import com.amazonaws.services.elasticmapreduce.model.JobFlowDetail;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.KeyValue;
import com.amazonaws.services.elasticmapreduce.model.MarketType;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepDetail;
import com.amazonaws.services.elasticmapreduce.model.StepExecutionState;
import com.amazonaws.services.elasticmapreduce.model.StepExecutionStatusDetail;
import com.amazonaws.services.elasticmapreduce.model.TerminateJobFlowsRequest;
import com.amazonaws.services.elasticmapreduce.util.BootstrapActions;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.math3.util.FastMath;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.common.ReloadingReference;
import net.myrrix.store.MyrrixConfiguration;
import net.myrrix.store.MyrrixSysPropertyCredentialsProvider;
import net.myrrix.store.Namespaces;
import net.myrrix.store.S3Utils;

/**
 * An implementation specialized for Amazon EMR, which can run a series of {@link JobStep}s as one or more
 * {@code JobFlow}s on Amazon EMR.
 * 
 * @author Sean Owen
 * @since 1.0
 */
public final class DelegateJob extends MyrrixJob {

  private static final Logger log = LoggerFactory.getLogger(DelegateJob.class.getName());

  public static final String BATCH_EMR_ZONE_KEY = "batch.emr.zone";
  public static final String WORKERS_COUNT_KEY = "batch.emr.workers.count";
  public static final String SUPPORTED_PRODUCT_KEY = "batch.emr.supportedProduct";
  public static final String WORKERS_TASKS_COUNT_KEY = "batch.emr.workers.tasks.count";
  public static final String WORKERS_TYPE_KEY = "batch.emr.workers.type";
  public static final String WORKERS_BID_KEY = "batch.emr.workers.bid";
  public static final String DEBUG_KEY = "batch.emr.debug";
  private static final String REDUCE_TASKS_KEY = "mapred.reduce.tasks";

  private static final InstanceType DEFAULT_INSTANCE_TYPE = InstanceType.M1_XLARGE;
  private static final URI MANAGEMENT_URL = URI.create("https://console.aws.amazon.com/elasticmapreduce/home");
  private static final long CHECK_INTERVAL_MS = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);

  private String jobFlowID;
  private final ReloadingReference<JobFlowDetail> cachedJobFlowDetail;
  private final AmazonElasticMapReduceClient emrClient;
  private final AmazonEC2Client ec2Client;

  public DelegateJob() {
    AWSCredentialsProvider credentialsProvider = S3Utils.getAWSCredentialsProvider();
    ClientConfiguration clientConfiguration = S3Utils.buildClientConfiguration();
    Region region = RegionUtils.getRegion(System.getProperty(BATCH_EMR_ZONE_KEY, "us-east-1"));
    emrClient = region.createClient(AmazonElasticMapReduceClient.class, credentialsProvider, clientConfiguration);
    ec2Client = region.createClient(AmazonEC2Client.class, credentialsProvider, clientConfiguration);

    cachedJobFlowDetail = new ReloadingReference<JobFlowDetail>(new Callable<JobFlowDetail>() {
      @Override
      public JobFlowDetail call() {
        DescribeJobFlowsResult describeJobFlowsResult = 
            emrClient.describeJobFlows(new DescribeJobFlowsRequest(Collections.singletonList(jobFlowID)));
        List<JobFlowDetail> jobFlowDetails = describeJobFlowsResult.getJobFlows();
        Preconditions.checkState(jobFlowDetails != null && jobFlowDetails.size() == 1,
                                 "Bad job flows state: %s", jobFlowDetails);
        return jobFlowDetails.get(0);
      }
    }, 2, TimeUnit.MINUTES);
  }

  @Override
  public MyrrixJobState getState() {
    MyrrixJobState state = new MyrrixJobState();
    if (jobFlowID != null) {
      for (StepDetail stepDetail : cachedJobFlowDetail.get().getSteps()) {
        StepExecutionStatusDetail executionDetail = stepDetail.getExecutionStatusDetail();
        MyrrixStepStatus stepStatus = translateStepStateToStatus(executionDetail.getState());
        Date startTime = executionDetail.getStartDateTime();
        Date endTime = executionDetail.getEndDateTime();
        String name = stepDetail.getStepConfig().getName();
        MyrrixStepState stepState = new MyrrixStepState(startTime, endTime, name, stepStatus);
        stepState.setManagementURI(MANAGEMENT_URL);
        if (stepStatus == MyrrixStepStatus.COMPLETED) {
          stepState.setMapProgress(1.0f);
          stepState.setReduceProgress(1.0f);
        }
        state.addStepState(stepState);
      }
    }
    state.setManagementURI(MANAGEMENT_URL);
    return state;
  }

  @Override
  public Void call() throws IOException, JobException, InterruptedException {

    boolean debug = Boolean.valueOf(System.getProperty(DEBUG_KEY));

    DeployRemoteJarFile.maybeUploadMyrrixJar(getInstanceID());

    JobFlowInstancesConfig jobFlowConfig = buildJobFlowConfig();

    try {
      RunJobFlowResult runJobFlowResult = emrClient.runJobFlow(buildRunJobFlow(debug, jobFlowConfig));
      jobFlowID = runJobFlowResult.getJobFlowId();

      log.info("Job flow {} is now running. If this process does not terminate normally, it may be unable " +
               "to terminate the EMR Job Flow. It will continue running (and costing money!) until manually " +
               "terminated", jobFlowID);

      List<StepConfig> preSteps = buildPreSteps(debug);
      emrClient.addJobFlowSteps(new AddJobFlowStepsRequest(jobFlowID, preSteps));
      MyrrixStepStatus status = await(0);

      if (status == MyrrixStepStatus.COMPLETED) {

        int iterationNumber = getStartIteration();

        if (Boolean.parseBoolean(System.getProperty("model.als.iterate", "true"))) {

          // Normal case: iterate
          boolean converged = false;
          while (!converged) {
            List<StepConfig> iterationSteps = buildIterationSteps(iterationNumber);
            long iterationStart = latestStartTime();
            emrClient.addJobFlowSteps(new AddJobFlowStepsRequest(jobFlowID, iterationSteps));
            status = await(iterationStart);
            if (status != MyrrixStepStatus.COMPLETED) {
              break;
            }
            log.info("Finished iteration {}", iterationNumber);
            if (checkConvergence(iterationNumber)) {
              converged = true;
            } else {
              iterationNumber++;
            }
          }

        } else {

          // Special case: just make one iteration to compute X from Y
          List<StepConfig> iterationSteps = buildIterationStepsX(iterationNumber);
          long iterationStart = latestStartTime();
          emrClient.addJobFlowSteps(new AddJobFlowStepsRequest(jobFlowID, iterationSteps));
          status = await(iterationStart);

        }

        if (status == MyrrixStepStatus.COMPLETED) {
          List<StepConfig> postSteps = buildPostSteps(iterationNumber);
          long postStart = latestStartTime();
          emrClient.addJobFlowSteps(new AddJobFlowStepsRequest(jobFlowID, postSteps));
          status = await(postStart);
        }

      }

      // Should job flow be cancelled on InterruptedException here in this case?

      if (status == MyrrixStepStatus.CANCELLED || status == MyrrixStepStatus.FAILED) {
        String errorMessage = "Job failed in state " + status + ". This is almost always due to earlier " +
                              "errors. See job logs for details.";
        log.error(errorMessage);
        throw new JobException(errorMessage);
      } else {
        Preconditions.checkState(status == MyrrixStepStatus.COMPLETED, "Status is not COMPLETED: %s", status);
        log.info("Job completed successfully. The Amazon EMR console will show status TERMINATED, which is normal");
      }

    } finally {
      emrClient.terminateJobFlows(new TerminateJobFlowsRequest(Collections.singletonList(jobFlowID)));
    }

    return null;
  }

  @Override
  public void close() {
    AmazonWebServiceClient theEMRClient = emrClient;
    if (theEMRClient != null) {
      theEMRClient.shutdown();
    }
    AmazonWebServiceClient theEC2Client = ec2Client;
    if (theEC2Client != null) {
      theEC2Client.shutdown();
    }
  }

  private List<StepConfig> buildPreSteps(boolean debug) {
    List<StepConfig> steps = Lists.newArrayList();
    // Needed to be able to debug and retrieve logs on Amazon EMR:
    if (debug) {
      steps.add(new StepConfig("Enable Debugging", new StepFactory().newEnableDebuggingStep()));
    }
    for (Collection<Class<? extends JobStep>> preStepClasses : getPreSchedule()) {
      steps.add(buildStepConfig(preStepClasses, 0, false));
    }
    return steps;
  }
  
  private List<StepConfig> buildIterationSteps(int iterationNumber) {
    List<StepConfig> steps = Lists.newArrayList();
    steps.addAll(buildIterationStepsX(iterationNumber));
    steps.addAll(buildIterationStepsY(iterationNumber));    
    return steps;
  }
  
  private List<StepConfig> buildIterationStepsX(int iterationNumber) {
    List<StepConfig> steps = Lists.newArrayList();
    Iterable<Collection<Class<? extends JobStep>>> iterationSchedule = getIterationSchedule();
    for (Collection<Class<? extends JobStep>> iterationStepClasses : iterationSchedule) {
      steps.add(buildStepConfig(iterationStepClasses, iterationNumber, true));  // X
    }
    return steps;
  }
  
  private List<StepConfig> buildIterationStepsY(int iterationNumber) {
    List<StepConfig> steps = Lists.newArrayList();
    Iterable<Collection<Class<? extends JobStep>>> iterationSchedule = getIterationSchedule();
    for (Collection<Class<? extends JobStep>> iterationStepClasses : iterationSchedule) {
      steps.add(buildStepConfig(iterationStepClasses, iterationNumber, false)); // Y
    }
    return steps;
  }

  private List<StepConfig> buildPostSteps(int finalIteration) {
    List<StepConfig> steps = Lists.newArrayList();
    for (Collection<Class<? extends JobStep>> postStepClasses : getPostSchedule()) {
      steps.add(buildStepConfig(postStepClasses, finalIteration, false));
    }
    return steps;
  }

  private RunJobFlowRequest buildRunJobFlow(boolean debug, JobFlowInstancesConfig jobFlowConfig) {
    String instanceID = getInstanceID();
    RunJobFlowRequest runJobFlowRequest =
        new RunJobFlowRequest("Myrrix-" + instanceID + '-' + getGenerationID(), jobFlowConfig);
    if (debug) {
      runJobFlowRequest.setLogUri(
          Namespaces.toPath(Namespaces.getLogsPrefix(instanceID, getGenerationID())).toString());
    }

    // Not sure whether to doc this yet
    runJobFlowRequest.setAmiVersion(System.getProperty("batch.emr.ami.version", "2.4.2"));

    String supportedProduct = System.getProperty(SUPPORTED_PRODUCT_KEY);
    if (supportedProduct != null) {
      runJobFlowRequest.setSupportedProducts(Collections.singleton(supportedProduct));
    }

    InstanceType instanceType = null;
    for (InstanceGroupConfig groupConfig : jobFlowConfig.getInstanceGroups()) {
      if (groupConfig.getName().contains("Core")) {
        instanceType = InstanceType.forInstanceName(groupConfig.getInstanceType());
        break;
      }
    }
    Preconditions.checkNotNull(instanceType);

    if (!Boolean.valueOf(System.getProperty("batch.emr.bootstrap.disable", "false"))) {
      addBootstrap(runJobFlowRequest, instanceType);
    }

    return runJobFlowRequest;
  }

  private static void addBootstrap(RunJobFlowRequest runJobFlowRequest, InstanceType instanceType) {
    int heapMB = desiredWorkerHeapSizeMB(instanceType);
    BootstrapActions.ConfigureHadoop configureHadoop = new BootstrapActions().newConfigureHadoop()
      .withKeyValue(BootstrapActions.ConfigFile.Mapred,
                    "mapred.map.child.java.opts", "-XX:+UseCompressedOops -XX:+UseParallelOldGC -Xmx512m")
      .withKeyValue(BootstrapActions.ConfigFile.Mapred,
                    "mapred.reduce.child.java.opts", "-XX:+UseCompressedOops -XX:+UseParallelOldGC -Xmx" + heapMB + 'm')
      .withKeyValue(BootstrapActions.ConfigFile.Mapred,
                    "mapred.job.reuse.jvm.num.tasks", "-1")
      .withKeyValue(BootstrapActions.ConfigFile.Mapred,
                    "mapreduce.job.jvm.numtasks", "-1")
      .withKeyValue(BootstrapActions.ConfigFile.Mapred,
                    "mapreduce.tasktracker.outofband.heartbeat", "true")
      .withKeyValue(BootstrapActions.ConfigFile.Mapred,
                    "mapred.compress.map.output", "true")
      .withKeyValue(BootstrapActions.ConfigFile.Mapred,
                    "mapred.map.tasks.speculative.execution", "true")
      .withKeyValue(BootstrapActions.ConfigFile.Mapred,
                    "mapred.reduce.tasks.speculative.execution", "true")
      .withKeyValue(BootstrapActions.ConfigFile.Mapred,
                    "mapred.max.split.size", Long.toString(1L << 30))
      .withKeyValue(BootstrapActions.ConfigFile.Mapred,
                    "mapred.min.split.size", Long.toString(1L << 27))
      .withKeyValue(BootstrapActions.ConfigFile.Mapred,
                    "mapred.output.compress", "true")
      // Note this is different! BLOCK doesn't work on S3 when reading a sequence file directly?
      .withKeyValue(BootstrapActions.ConfigFile.Mapred,
                    "mapred.output.compression.type", SequenceFile.CompressionType.RECORD.toString())
      .withKeyValue(BootstrapActions.ConfigFile.Hdfs,
                    "dfs.block.size", "67108864")
      .withKeyValue(BootstrapActions.ConfigFile.Core,
                    "io.file.buffer.size", "131072")
      //.withKeyValue(BootstrapActions.ConfigFile.Core,
      //              "io.bytes.per.checksum", "8192")
      .withKeyValue(BootstrapActions.ConfigFile.Core,
                    "io.sort.factor", "32")
      .withKeyValue(BootstrapActions.ConfigFile.Core,
                    "io.sort.mb", "200");
    String mapredReduceTasks = System.getProperty(REDUCE_TASKS_KEY);
    if (mapredReduceTasks != null) {
      configureHadoop =
          configureHadoop.withKeyValue(BootstrapActions.ConfigFile.Mapred, REDUCE_TASKS_KEY, mapredReduceTasks);
    }
    runJobFlowRequest.setBootstrapActions(Collections.singletonList(configureHadoop.build()));
  }

  private static int desiredWorkerHeapSizeMB(InstanceType type) {
    int instanceRAM = type.getRamMB();
    int defaultReducers = type.getReducers();
    log.info("Assuming {}MB heap and {} reducers for type {}", instanceRAM, defaultReducers, type);
    int overhead = type == InstanceType.M1_SMALL ? 128 : 384;
    // Use this for task tracker and data node size too
    int available = instanceRAM - 2 * overhead;
    available = (int) (0.9 * available); // Cushion
    int perReducer = available / defaultReducers;
    log.info("Using {}MB per reducer", perReducer);
    return perReducer;
  }

  private JobFlowInstancesConfig buildJobFlowConfig() {
    JobFlowInstancesConfig jobFlowConfig = new JobFlowInstancesConfig();

    // Necessary to be able to dynamically add steps
    jobFlowConfig.setKeepJobFlowAliveWhenNoSteps(true);

    // Not sure whether to doc this, yet
    String hadoopVersion = System.getProperty("batch.emr.hadoop.version");
    if (hadoopVersion != null) {
      jobFlowConfig.setHadoopVersion(hadoopVersion);
    }

    // Not sure whether to doc this, yet
    String zonePlacement = System.getProperty("batch.emr.zone.placement");
    if (zonePlacement != null) {
      jobFlowConfig.setPlacement(new PlacementType(zonePlacement));
    }

    int numWorkers = Integer.parseInt(System.getProperty(WORKERS_COUNT_KEY, "1"));
    Preconditions.checkArgument(numWorkers > 0, "Workers must be positive: %s", numWorkers);
    int numTaskWorkers = Integer.parseInt(System.getProperty(WORKERS_TASKS_COUNT_KEY, "0"));
    Preconditions.checkArgument(numTaskWorkers >= 0, "Num task workers must not be negative: %s", numTaskWorkers);
    int numCoreWorkers = numWorkers - numTaskWorkers;
    Preconditions.checkArgument(numCoreWorkers > 0, "Num core workers must be positive: %s", numCoreWorkers);

    Collection<InstanceGroupConfig> configs = Lists.newArrayList();

    InstanceGroupConfig masterConfig = new InstanceGroupConfig();
    masterConfig.setName("Myrrix Master");
    masterConfig.setInstanceCount(1);
    masterConfig.setInstanceRole(InstanceRoleType.MASTER);
    masterConfig.setMarket(MarketType.ON_DEMAND);

    if (numWorkers > 4) {  // A bit arbitrary
      log.info("Using {} instance for master because of cluster size", InstanceType.M1_LARGE);
      masterConfig.setInstanceType(InstanceType.M1_LARGE.getInstanceName());
    } else {
      masterConfig.setInstanceType(InstanceType.M1_SMALL.getInstanceName());
    }

    configs.add(masterConfig);

    String workersBidString = System.getProperty(WORKERS_BID_KEY);
    boolean autoBid = "auto".equals(workersBidString);
    BigDecimal workersBid = autoBid || workersBidString == null ? null : new BigDecimal(workersBidString);

    if (workersBid != null) {
      Preconditions.checkArgument(workersBid.compareTo(BigDecimal.ZERO) > 0, "Bid must be positive: %s", workersBid);
      workersBid = workersBid.stripTrailingZeros();
      if (workersBid.scale() > 3) {
        // Amazon limit; round bid down from user's minimum
        workersBid = workersBid.setScale(3, RoundingMode.DOWN);
        log.warn("Truncating worker bid to {} due to Amazon limit", workersBid);
      }
    }

    String workerTypeString = System.getProperty(WORKERS_TYPE_KEY, DEFAULT_INSTANCE_TYPE.getInstanceName());
    InstanceType workerType = InstanceType.forInstanceName(workerTypeString);
    if (workerType == null) {
      log.warn("Unsupported or unknown instance type {}, defaulting to {}", workerTypeString, DEFAULT_INSTANCE_TYPE);
      workerType = DEFAULT_INSTANCE_TYPE;
    }

    InstanceGroupConfig coreWorkerConfig = new InstanceGroupConfig();
    coreWorkerConfig.setName("Myrrix Core");
    coreWorkerConfig.setInstanceCount(numCoreWorkers);
    coreWorkerConfig.setInstanceRole(InstanceRoleType.CORE);
    coreWorkerConfig.setInstanceType(workerType.getInstanceName());

    InstanceGroupConfig taskWorkerConfig = null;
    if (numTaskWorkers > 0) {
      taskWorkerConfig = new InstanceGroupConfig();
      taskWorkerConfig.setName("Myrrix Task");
      taskWorkerConfig.setInstanceCount(numTaskWorkers);
      taskWorkerConfig.setInstanceRole(InstanceRoleType.TASK);
      taskWorkerConfig.setInstanceType(workerType.getInstanceName());
    }

    if (workersBid == null) {
      coreWorkerConfig.setMarket(MarketType.ON_DEMAND);
      if (numTaskWorkers > 0) {
        BigDecimal bestSpotPrice = getLowestCurrentSpotPrice(workerType.getInstanceName());
        if (bestSpotPrice == null) {
          log.warn("Can't locate any bid info for task worker bid; using on-demand instance for task workers");
          taskWorkerConfig.setMarket(MarketType.ON_DEMAND);
        } else {
          taskWorkerConfig.setBidPrice(bestSpotPrice.toPlainString());
          taskWorkerConfig.setMarket(MarketType.SPOT);
        }
      }
    } else {
      BigDecimal bestSpotPrice = getLowestCurrentSpotPrice(workerType.getInstanceName());
      if (bestSpotPrice == null) {
        if (autoBid) {
          throw new IllegalStateException(
              "Auto bid selected, but could not read spot prices from EC2! Specify bid manually");
        } else {
          log.info("Could not read spot prices from EC2, proceeding with specified bid of {}", workersBid);
        }
      } else {
        if (autoBid) {
          BigDecimal bidOverSpotPrice = bestSpotPrice.multiply(new BigDecimal(2));
          log.info("Spot price is {}, bidding up to 2x: {}", bestSpotPrice, bidOverSpotPrice);
          workersBid = bidOverSpotPrice;
        } else if (workersBid.compareTo(bestSpotPrice) < 0) {
          log.warn("Bid is under current lowest spot price of {}; adjusting upwards to match", bestSpotPrice);
          workersBid = bestSpotPrice;
        }
      }
      coreWorkerConfig.setMarket(MarketType.SPOT);
      coreWorkerConfig.setBidPrice(workersBid.toPlainString());
      if (numTaskWorkers > 0) {
        taskWorkerConfig.setMarket(MarketType.SPOT);
        taskWorkerConfig.setBidPrice(workersBid.toPlainString());
      }
    }

    configs.add(coreWorkerConfig);
    if (taskWorkerConfig != null) {
      configs.add(taskWorkerConfig);
    }

    jobFlowConfig.setInstanceGroups(configs);

    int reducerSlotsPerWorker = workerType.getReducers();
    int totalReduceSlots = numWorkers * reducerSlotsPerWorker;
    int currentReduceParallelism = Integer.parseInt(System.getProperty(REDUCE_TASKS_KEY, "1"));
    if (currentReduceParallelism < totalReduceSlots) {
      log.info("Increasing {} to {} because cluster has {} workers and {} slots per worker",
               REDUCE_TASKS_KEY, totalReduceSlots, numWorkers, reducerSlotsPerWorker);
      System.setProperty(REDUCE_TASKS_KEY, Integer.toString(totalReduceSlots));
    }

    return jobFlowConfig;
  }

  private BigDecimal getLowestCurrentSpotPrice(String instanceType) {
    DescribeSpotPriceHistoryRequest historyRequest = new DescribeSpotPriceHistoryRequest();
    historyRequest.setInstanceTypes(Collections.singleton(instanceType));
    historyRequest.setProductDescriptions(Collections.singleton("Linux/UNIX"));
    historyRequest.setMaxResults(24);
    historyRequest.setStartTime(
        new Date(System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(3, TimeUnit.DAYS)));
    DescribeSpotPriceHistoryResult historyResult = ec2Client.describeSpotPriceHistory(historyRequest);

    Map<String,Pair<Long,BigDecimal>> mostRecentByAZ = Maps.newHashMap();
    for (SpotPrice spotPrice : historyResult.getSpotPriceHistory()) {
      String zone = spotPrice.getAvailabilityZone();
      long timestamp = spotPrice.getTimestamp().getTime();
      Pair<Long,BigDecimal> mostRecent = mostRecentByAZ.get(zone);
      if (mostRecent == null || timestamp > mostRecent.getFirst()) {
        mostRecentByAZ.put(zone, new Pair<Long,BigDecimal>(timestamp, new BigDecimal(spotPrice.getSpotPrice())));
      }
    }

    BigDecimal lowestPrice = null;
    for (Pair<Long,BigDecimal> mostRecent : mostRecentByAZ.values()) {
      if (lowestPrice == null || mostRecent.getSecond().compareTo(lowestPrice) < 0) {
        lowestPrice = mostRecent.getSecond();
      }
    }
    if (lowestPrice == null) {
      log.info("Could not read any spot price for instance type {}", instanceType);
    } else {
      lowestPrice = lowestPrice.stripTrailingZeros();
      log.info("Lowest spot price in region for instance type {} is {}", instanceType, lowestPrice);
      if (lowestPrice.scale() > 3) {
        // Amazon limit; round lowest price *up*
        lowestPrice = lowestPrice.setScale(3, RoundingMode.UP);
        log.warn("Rounding up to {} due to Amazon limit", lowestPrice);
      }
    }
    return lowestPrice;
  }

  private StepConfig buildStepConfig(Collection<Class<? extends JobStep>> parallelStepClasses,
                                     int iteration,
                                     boolean computingX) {

    HadoopJarStepConfig hadoopJarStepConfig =
        new HadoopJarStepConfig(Namespaces.toPath(Namespaces.getMyrrixJarPrefix(getInstanceID())).toString());

    // Pass these on the command line to individual steps so they're aware of what instance, generation
    // they're working on.
    JobStepConfig config = new JobStepConfig(Namespaces.get().getBucket(),
                                             getInstanceID(),
                                             getGenerationID(),
                                             getLastGenerationID(),
                                             iteration,
                                             computingX);
    Collection<String> args = Lists.newArrayList(config.toArgsArray());

    String configName;

    if (parallelStepClasses.size() > 1) {

      StringBuilder parallelStepClassNames = new StringBuilder();
      StringBuilder shortNames = new StringBuilder();
      for (Class<? extends Tool> parallelStepClass : parallelStepClasses) {
        if (parallelStepClassNames.length() > 0) {
          parallelStepClassNames.append(',');
          shortNames.append(',');
        }
        parallelStepClassNames.append(parallelStepClass.getName());
        shortNames.append(parallelStepClass.getSimpleName());
      }
      args.add(parallelStepClassNames.toString());

      hadoopJarStepConfig.setMainClass(ParallelStep.class.getName());
      if (shortNames.length() > 256) {
        configName = shortNames.substring(0, 256); // To meet Amazon limit
      } else {
        configName = shortNames.toString();
      }

    } else {

      Class<? extends Tool> stepClass = parallelStepClasses.iterator().next();
      hadoopJarStepConfig.setMainClass(stepClass.getName());
      configName = stepClass.getSimpleName();

    }

    hadoopJarStepConfig.setArgs(args);
    addSystemProperties(hadoopJarStepConfig);
    return new StepConfig(configName, hadoopJarStepConfig);
  }

  private static void addSystemProperties(HadoopJarStepConfig hadoopJarStepConfig) {
    AWSCredentials credentials = S3Utils.getAWSCredentialsProvider().getCredentials();
    Collection<KeyValue> systemProperties = Lists.newArrayList(
        new KeyValue(MyrrixSysPropertyCredentialsProvider.ACCESS_KEY_PROPERTY, credentials.getAWSAccessKeyId()),
        new KeyValue(MyrrixSysPropertyCredentialsProvider.SECRET_KEY_PROPERTY, credentials.getAWSSecretKey()));
    String mapredReduceTasks = System.getProperty(REDUCE_TASKS_KEY);
    if (mapredReduceTasks != null) {
      systemProperties.add(new KeyValue(REDUCE_TASKS_KEY, mapredReduceTasks));
    }
    for (String key : MyrrixConfiguration.getSystemPropertiesToForward()) {
      String value = System.getProperty(key);
      if (value != null) {
        systemProperties.add(new KeyValue(key, value));
      }
    }
    hadoopJarStepConfig.setProperties(systemProperties);
  }

  /**
   * @return final state
   */
  private MyrrixStepStatus await(long after) throws InterruptedException {    
    int countDownToLog = 0;
    Collection<String> completedSteps = Lists.newArrayList();
    Collection<String> runningOrPendingSteps = Lists.newArrayList();
    
    while (completedSteps.isEmpty() || !runningOrPendingSteps.isEmpty()) {
      Thread.sleep(CHECK_INTERVAL_MS);

      completedSteps.clear();
      runningOrPendingSteps.clear();
      cachedJobFlowDetail.clear();
      
      for (StepDetail stepDetail : cachedJobFlowDetail.get().getSteps()) {
        String name = stepDetail.getStepConfig().getName();        
        StepExecutionStatusDetail stepStatus = stepDetail.getExecutionStatusDetail();
        MyrrixStepStatus status = translateStepStateToStatus(stepStatus.getState());
        
        // Check for *any* previous failed or cancelled to be safe
        if (status == MyrrixStepStatus.CANCELLED || status == MyrrixStepStatus.FAILED) {
          log.error("Step {} failed in state {}", name, stepStatus);
          return status;
        }
        // Use time to avoid ambiguity with things like RowStep
        Date startDateTime = stepStatus.getStartDateTime();
        if (startDateTime != null && startDateTime.getTime() > after) {
          if (status == MyrrixStepStatus.COMPLETED) {
            completedSteps.add(name);
          } else if (status == MyrrixStepStatus.PENDING || status == MyrrixStepStatus.RUNNING) {
            runningOrPendingSteps.add(name);
          }
        }
      }

      if (countDownToLog == 0) {
        log.info("Job {} is running or waiting to run: {}; completed: {}", 
                 getClass().getSimpleName(), runningOrPendingSteps, completedSteps);
        countDownToLog = 5;
      } else {
        countDownToLog--;
      }
    }
    return MyrrixStepStatus.COMPLETED;
  }
  
  private long latestStartTime() {
    cachedJobFlowDetail.clear();
    long latest = 0;
    for (StepDetail stepDetail : cachedJobFlowDetail.get().getSteps()) {
      Date startTime = stepDetail.getExecutionStatusDetail().getStartDateTime();
      if (startTime != null) {
        latest = FastMath.max(latest, startTime.getTime());
      }
    }
    Preconditions.checkState(latest > 0, "No previous steps with a start time?");
    return latest;
  }

  private static MyrrixStepStatus translateStepStateToStatus(String emrStepState) {
    switch (StepExecutionState.fromValue(emrStepState)) {
      case PENDING:
        return MyrrixStepStatus.PENDING;
      case RUNNING:
      case CONTINUE:
        return MyrrixStepStatus.RUNNING;
      case COMPLETED:
        return MyrrixStepStatus.COMPLETED;
      case CANCELLED:
      case INTERRUPTED:
        return MyrrixStepStatus.CANCELLED;
      case FAILED:
        return MyrrixStepStatus.FAILED;
    }
    throw new IllegalArgumentException(emrStepState);
  }

}
