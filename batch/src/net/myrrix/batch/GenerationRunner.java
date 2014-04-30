/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.util.FastMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.batch.common.DependenciesScheduler;
import net.myrrix.batch.common.DependsOn;
import net.myrrix.batch.initialy.InitialYStep;
import net.myrrix.batch.iterate.cluster.ClusterStep;
import net.myrrix.batch.iterate.row.RowStep;
import net.myrrix.batch.known.CollectKnownItemsStep;
import net.myrrix.batch.merge.MergeNewOldStep;
import net.myrrix.batch.merge.ToItemVectorsStep;
import net.myrrix.batch.merge.ToUserVectorsStep;
import net.myrrix.batch.merge.tag.MergeNewOldItemTagsStep;
import net.myrrix.batch.merge.tag.MergeNewOldUserTagsStep;
import net.myrrix.batch.mergemodel.AssembleYTXStep;
import net.myrrix.batch.mergemodel.MergeModelStep;
import net.myrrix.batch.mergemodel.MultiplyYTXStep;
import net.myrrix.batch.popular.PopularItemStep;
import net.myrrix.batch.popular.PopularUserStep;
import net.myrrix.batch.publish.PublishItemCentroidsStep;
import net.myrrix.batch.publish.PublishItemClusterStep;
import net.myrrix.batch.publish.PublishUserCentroidsStep;
import net.myrrix.batch.publish.PublishUserClusterStep;
import net.myrrix.batch.publish.PublishXStep;
import net.myrrix.batch.publish.PublishYStep;
import net.myrrix.batch.recommend.CollectRecommendStep;
import net.myrrix.batch.recommend.DistributeRecommendWorkStep;
import net.myrrix.batch.recommend.RecommendStep;
import net.myrrix.batch.similar.DistributeSimilarWorkStep;
import net.myrrix.batch.similar.SimilarStep;
import net.myrrix.common.ClassUtils;
import net.myrrix.common.LangUtils;
import net.myrrix.common.math.MatrixUtils;
import net.myrrix.store.Namespaces;
import net.myrrix.store.Store;
import net.myrrix.store.StoreUtils;

/**
 * <p>This class will run one generation of the Computation Layer. It is essentially a client of a Hadoop cluster
 * and starts and monitors a series of MapReduce jobs that complete the Computation Layer's work.</p>
 *
 * @author Sean Owen
 * @since 1.0
 * @see PeriodicRunner
 */
public final class GenerationRunner implements Callable<Void> {

  private static final Logger log = LoggerFactory.getLogger(GenerationRunner.class);

  private static final Splitter ON_DELIMITER = Splitter.on('/');
  private static final Pattern COMMA = Pattern.compile(",");

  // See 2 minute interval in distributed version of DelegateGenerationManager
  private static final long GENERATION_WAIT = TimeUnit.MILLISECONDS.convert(4, TimeUnit.MINUTES);
  private static final int MAX_GENERATIONS_TO_KEEP = 
      FastMath.max(2, Integer.parseInt(System.getProperty("model.generations.maxToKeep", "10")));

  private final PeriodicRunnerConfig config;
  private long generationID;
  private long lastGenerationID;
  private MyrrixJob myrrixJob;
  private Date startTime;
  private Date endTime;
  private boolean isRunning;

  public GenerationRunner(PeriodicRunnerConfig config) {
    Preconditions.checkNotNull(config);
    this.config = config;
    generationID = -1;
    lastGenerationID = -1;
    myrrixJob = null;
  }

  /**
   * @return current state of execution at a {@link GenerationState}, or {@code null} if no job has been
   *  started
   */
  public GenerationState getState() throws IOException {
    if (generationID < 0) {
      return null;
    }
    MyrrixJobState jobState = myrrixJob == null ? null : myrrixJob.getState();
    return new GenerationState(generationID, jobState, isRunning, startTime, endTime);
  }

  @Override
  public Void call() throws IOException, JobException, InterruptedException {
    isRunning = true;
    startTime = new Date();
    try {

      String instanceID = config.getInstanceID();

      Collection<String> runningJobs;
      do {
        FindRunningJobs findRunningJobs =
            ClassUtils.loadInstanceOf("net.myrrix.batch.FindRunningJobsImpl", FindRunningJobs.class);
        runningJobs = findRunningJobs.find(instanceID);
        if (!runningJobs.isEmpty()) {
          log.warn("Jobs are already running for instance {}, waiting: {}", instanceID, runningJobs);
          Thread.sleep(TimeUnit.MILLISECONDS.convert(2, TimeUnit.MINUTES));
        }
      } while (!runningJobs.isEmpty());

      log.info("Starting run for instance ID {}", instanceID);
      runGeneration();
      return null;

    } finally {
      isRunning = false;
      endTime = new Date();
    }
  }

  private void runGeneration() throws IOException, JobException, InterruptedException {
    List<String> generationStrings = StoreUtils.listGenerationsForInstance(config.getInstanceID());
    log.info("Recent generations: {}", generationStrings);

    Store store = Store.get();
    int count = generationStrings.size();    
    if (count > MAX_GENERATIONS_TO_KEEP) {
      Iterable<String> generationsToDelete = generationStrings.subList(0, count - MAX_GENERATIONS_TO_KEEP);
      log.info("Deleting old generations: {}", generationsToDelete);
      for (String generationPrefix : generationsToDelete) {
        store.recursiveDelete(generationPrefix);
      }
    }

    String instanceID = config.getInstanceID();

    long lastDoneGeneration = -1;
    int lastDoneGenerationIndex = -1;
    for (int i = count - 1; i >= 0; i--) {
      long generationID = parseGenerationFromPrefix(generationStrings.get(i));
      if (store.exists(Namespaces.getGenerationDoneKey(instanceID, generationID), true)) {
        lastDoneGeneration = generationID;
        lastDoneGenerationIndex = i;
        break;
      }
    }

    lastGenerationID = lastDoneGeneration;

    long generationToMake;
    long generationToWaitFor;
    long generationToRun;

    if (lastDoneGeneration >= 0) {

      log.info("Last complete generation is {}", lastDoneGeneration);
      if (lastDoneGenerationIndex == count - 1) {
        generationToMake = lastDoneGeneration + 1;
        generationToRun = -1;
        generationToWaitFor = -1;
      } else if (lastDoneGenerationIndex == count - 2) {
        generationToRun = parseGenerationFromPrefix(generationStrings.get(lastDoneGenerationIndex + 1));
        generationToMake = generationToRun + 1;
        generationToWaitFor = generationToMake;
      } else {
        generationToRun = parseGenerationFromPrefix(generationStrings.get(lastDoneGenerationIndex + 1));
        generationToMake = -1;
        generationToWaitFor = parseGenerationFromPrefix(generationStrings.get(lastDoneGenerationIndex + 2));
      }

    } else {

      log.info("No complete generations");
      // If nothing is done,
      if (generationStrings.isEmpty()) {
        // and no generations exist, make one
        generationToRun = -1;
        generationToMake = 0;
        generationToWaitFor = -1;
      } else {
        if (count >= 2) {
          // Run current one and open a next generation
          generationToRun = parseGenerationFromPrefix(generationStrings.get(0));
          generationToMake = -1;
          generationToWaitFor = parseGenerationFromPrefix(generationStrings.get(1));
        } else {
          generationToRun = parseGenerationFromPrefix(generationStrings.get(0));
          generationToMake = generationToRun + 1;
          generationToWaitFor = generationToMake;
        }
      }

    }

    Preconditions.checkState((generationToWaitFor < 0) == (generationToRun < 0),
                             "There must either be both a generation to wait for and generation " +
                             "to run, or neither");

    if (generationToMake < 0) {
      log.info("No need to make a new generation");
    } else {
      log.info("Making new generation {}", generationToMake);
      store.mkdir(Namespaces.getInboundPrefix(instanceID, generationToMake));
    }

    // Set this now before starting to wait -- so we don't show 0 generations in the console
    if (generationToRun >= 0) {
      generationID = generationToRun;
    }

    maybeWaitToRun(instanceID, generationToWaitFor, generationToRun);

    if (generationToRun < 0) {
      log.info("No generation to run");
    } else {
      generationID = generationToRun;
      log.info("Running generation {}", generationID);
      runSteps();
      log.info("Signaling completion of generation {}", generationID);
      store.touch(Namespaces.getGenerationDoneKey(instanceID, generationID));
    }
  }

  private static void maybeWaitToRun(String instanceID,
                                     long generationToWaitFor,
                                     long generationToRun) throws IOException, InterruptedException {
    Store store = Store.get();
    if (generationToWaitFor < 0) {
      log.info("No need to wait for a generation");
      return;
    }

    String nextGenerationPrefix = Namespaces.getInboundPrefix(instanceID, generationToWaitFor);
    long lastModified = store.getLastModified(nextGenerationPrefix);
    long now = System.currentTimeMillis();
    if (now > lastModified + GENERATION_WAIT) {
      log.info("Generation {} is old enough to proceed", generationToWaitFor);
    } else {
      // Not long enough, wait
      long toSleepMS = lastModified + GENERATION_WAIT - now;
      log.info("Waiting {}s for data to start uploading to generation {} and then move to {}...",
               TimeUnit.SECONDS.convert(toSleepMS, TimeUnit.MILLISECONDS),
               generationToRun,
               generationToWaitFor);
      Thread.sleep(toSleepMS);
    }

    String uploadingGenerationPrefix = Namespaces.getInboundPrefix(instanceID, generationToRun);

    boolean anyInProgress = true;
    while (anyInProgress) {
      log.info("Waiting for uploads to finish in {}...", uploadingGenerationPrefix);
      anyInProgress = false;
      for (String fileName : store.list(uploadingGenerationPrefix, true)) {
        // .inprogress is our marker for uploading files; _COPYING_ is Hadoop's
        if (fileName.endsWith(".inprogress") || fileName.endsWith("_COPYING_")) {
          long inProgressLastModified = store.getLastModified(fileName);
          if (System.currentTimeMillis() < inProgressLastModified + 5 * GENERATION_WAIT) {
            log.info("At least one upload is in progress ({})", fileName);
            anyInProgress = true;
            break;
          } else {
            log.warn("Stale upload to {}? Deleting and continuing", fileName);
            try {
              store.delete(fileName);
            } catch (IOException ioe) {
              log.info("Could not delete {}", fileName, ioe);
            }
          }
        }
      }
      if (anyInProgress) {
        Thread.sleep(TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES));
      }
    }

  }

  private void runSteps() throws IOException, JobException, InterruptedException {

    MyrrixJob generationRunnerJob = MyrrixJob.get();
    generationRunnerJob.setInstanceID(config.getInstanceID());
    generationRunnerJob.setGenerationID(generationID);
    generationRunnerJob.setLastGenerationID(lastGenerationID);

    Collection<DependsOn<Class<? extends JobStep>>> preDeps = Lists.newArrayList();
    preDeps.add(DependsOn.nextAfterFirst(MergeNewOldItemTagsStep.class, MergeNewOldStep.class));
    preDeps.add(DependsOn.nextAfterFirst(MergeNewOldUserTagsStep.class, MergeNewOldStep.class));
    preDeps.add(DependsOn.nextAfterFirst(ToUserVectorsStep.class, MergeNewOldStep.class));
    preDeps.add(DependsOn.nextAfterFirst(ToItemVectorsStep.class, MergeNewOldStep.class));
    preDeps.add(DependsOn.nextAfterFirst(CollectKnownItemsStep.class, ToUserVectorsStep.class));
    preDeps.add(DependsOn.nextAfterFirst(InitialYStep.class, ToItemVectorsStep.class));    
    preDeps.add(DependsOn.nextAfterFirst(PopularUserStep.class, ToUserVectorsStep.class));
    preDeps.add(DependsOn.nextAfterFirst(PopularItemStep.class, ToItemVectorsStep.class));
    
    DependenciesScheduler<Class<? extends JobStep>> scheduler = new DependenciesScheduler<Class<? extends JobStep>>();
    
    List<Collection<Class<? extends JobStep>>> preSchedule = scheduler.schedule(preDeps);
    generationRunnerJob.setPreSchedule(preSchedule);

    Collection<DependsOn<Class<? extends JobStep>>> iterationsDeps = Lists.newArrayList();
    if (config.isCluster()) {
      iterationsDeps.add(DependsOn.<Class<? extends JobStep>>nextAfterFirst(ClusterStep.class, RowStep.class));
    } else {
      iterationsDeps.add(DependsOn.<Class<? extends JobStep>>first(RowStep.class));
    }
    List<Collection<Class<? extends JobStep>>> iterationSchedule = scheduler.schedule(iterationsDeps);
    generationRunnerJob.setIterationSchedule(iterationSchedule);

    int startingIteration = readLatestIterationInProgress();
    log.info("Starting from iteration {}", startingIteration);
    generationRunnerJob.setStartIteration(startingIteration);

    Collection<DependsOn<Class<? extends JobStep>>> postDeps = Lists.newArrayList();
    postDeps.add(DependsOn.<Class<? extends JobStep>>first(PublishXStep.class));
    postDeps.add(DependsOn.<Class<? extends JobStep>>first(PublishYStep.class));
    if (config.isCluster()) {
      postDeps.add(DependsOn.<Class<? extends JobStep>>first(PublishUserClusterStep.class));
      postDeps.add(DependsOn.<Class<? extends JobStep>>first(PublishItemClusterStep.class));
      postDeps.add(DependsOn.<Class<? extends JobStep>>first(PublishUserCentroidsStep.class));
      postDeps.add(DependsOn.<Class<? extends JobStep>>first(PublishItemCentroidsStep.class));
    }
    if (config.isRecommend()) {
      postDeps.add(DependsOn.<Class<? extends JobStep>>nextAfterFirst(RecommendStep.class, 
                                                                      DistributeRecommendWorkStep.class));
      postDeps.add(DependsOn.nextAfterFirst(CollectRecommendStep.class, RecommendStep.class));
    }
    if (config.isMakeItemSimilarity()) {
      postDeps.add(DependsOn.nextAfterFirst(SimilarStep.class, DistributeSimilarWorkStep.class));
    }
    if (System.getProperty(MultiplyYTXStep.MODEL_MERGE_INSTANCE_PROPERTY) != null) { // Experimental
      postDeps.add(DependsOn.nextAfterFirst(MultiplyYTXStep.class, PublishYStep.class));
      postDeps.add(DependsOn.nextAfterFirst(AssembleYTXStep.class, MultiplyYTXStep.class));
      postDeps.add(DependsOn.nextAfterFirst(MergeModelStep.class, AssembleYTXStep.class));
    }
    List<Collection<Class<? extends JobStep>>> postSchedule = scheduler.schedule(postDeps);
    generationRunnerJob.setPostSchedule(postSchedule);

    MyrrixJob oldJob = myrrixJob;
    if (oldJob != null) {
      oldJob.close();
    }
    myrrixJob = generationRunnerJob;
    generationRunnerJob.call();

    testXandYRank(generationRunnerJob);
    
    if (!Boolean.parseBoolean(System.getProperty("batch.debug", "false"))) {
      Store.get().recursiveDelete(Namespaces.getTempPrefix(config.getInstanceID(), generationID));
    }
  }

  private static void testXandYRank(MyrrixJob generationRunnerJob) throws IOException {
    log.info("Loading X and Y to test whether they have sufficient rank");

    String generationPrefix = Namespaces.getInstanceGenerationPrefix(generationRunnerJob.getInstanceID(),
                                                                     generationRunnerJob.getGenerationID());
    String xPrefix = generationPrefix + "X/";
    String yPrefix = generationPrefix + "Y/";

    if (!doesXorYHasSufficientRank(xPrefix) || !doesXorYHasSufficientRank(yPrefix)) {
      Store store = Store.get();      
      log.warn("X or Y does not have sufficient rank; deleting this model and its results");
      store.recursiveDelete(xPrefix);
      store.recursiveDelete(yPrefix);
      store.recursiveDelete(generationPrefix + "recommend/");
      store.recursiveDelete(generationPrefix + "similarItems/");
      store.recursiveDelete(generationPrefix + "itemClusters/");
      store.recursiveDelete(generationPrefix + "userClusters/");
    } else {
      log.info("X and Y have sufficient rank");
    }
  }

  private static boolean doesXorYHasSufficientRank(String xOrYPrefix) throws IOException {
    Iterable<String> xOrYFilePrefixes = Store.get().list(xOrYPrefix, true);
    double[][] transposeTimesSelf = null;
    for (String xOrYFilePrefix : xOrYFilePrefixes) {
      BufferedReader reader = Store.get().streamFrom(xOrYFilePrefix);
      try {
        String line;
        while ((line = reader.readLine()) != null) {

          int tab = line.indexOf('\t');
          Preconditions.checkArgument(tab >= 0, "Bad input line in %s: %s", xOrYFilePrefix, line);

          CharSequence featureVectorString = line.substring(tab + 1);
          String[] elementTokens = COMMA.split(featureVectorString);
          int features = elementTokens.length;

          if (transposeTimesSelf == null) {
            transposeTimesSelf = new double[features][features];
          }

          float[] elements = new float[features];
          for (int i = 0; i < features; i++) {
            elements[i] = LangUtils.parseFloat(elementTokens[i]);
          }

          for (int i = 0; i < features; i++) {
            double rowFactor = elements[i];
            double[] transposeTimesSelfRow = transposeTimesSelf[i];
            for (int j = 0; j < features; j++) {
              transposeTimesSelfRow[j] += rowFactor * elements[j];
            }
          }

        }
      } finally {
        reader.close();
      }
      
      // Hacky but efficient and principled: if a portion of the matrix times itself is non-singular,
      // it's all but impossible that it would be singular when the rest was loaded
      
      if (transposeTimesSelf != null && 
          transposeTimesSelf.length != 0 && 
          MatrixUtils.isNonSingular(new Array2DRowRealMatrix(transposeTimesSelf))) {
        return true;
      } else {
        log.info("Matrix is not yet proved to be non-singular, continuing to load...");
      }
    }
    return false;
  }

  private static long parseGenerationFromPrefix(CharSequence prefix) {
    Iterator<String> tokens = ON_DELIMITER.split(prefix).iterator();
    tokens.next();
    return Long.parseLong(tokens.next());
  }

  private int readLatestIterationInProgress() throws IOException {
    String iterationsPrefix = Namespaces.getIterationsPrefix(config.getInstanceID(), generationID);
    List<String> iterationPaths = Store.get().list(iterationsPrefix, false);
    if (iterationPaths == null || iterationPaths.isEmpty()) {
      return 1;
    }
    String iterationString = null;
    CharSequence lastIterationPath = iterationPaths.get(iterationPaths.size() - 1);
    for (String s : ON_DELIMITER.split(lastIterationPath)) {
      if (!s.isEmpty()) {
        iterationString = s;
      }
    }
    int iteration = Integer.parseInt(iterationString);
    if (iteration == 0) {
      // Iteration 0 is a fake iteration with initial Y; means we're really on 1
      iteration = 1;
    }
    return iteration;
  }

}
