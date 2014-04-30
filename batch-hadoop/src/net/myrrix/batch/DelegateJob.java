/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.common.ClassUtils;
import net.myrrix.store.MyrrixConfiguration;
import net.myrrix.store.Namespaces;

/**
 * Implementation that can run {@link JobStep}s on a Hadoop cluster.
 * 
 * @author Sean Owen
 * @since 1.0
 */
public final class DelegateJob extends MyrrixJob {

  private static final Logger log = LoggerFactory.getLogger(DelegateJob.class);

  private static final Pattern JOBTRACKER_PATTERN = Pattern.compile("([^:]+):");
  private static final Pattern JOBTRACKER_HTTP_ADDRESS_PATTERN = Pattern.compile(":([0-9]+)");

  private final Collection<HasState> stateSources;

  public DelegateJob() {
    stateSources = Lists.newCopyOnWriteArrayList();
  }

  @Override
  public MyrrixJobState getState() throws IOException {
    MyrrixJobState state = new MyrrixJobState();
    for (HasState stateSource : stateSources) {
      state.addStepStates(stateSource.getStepStates());
    }
    state.setManagementURI(getManagementURI());
    return state;
  }

  private static URI getManagementURI() {

    Configuration config = MyrrixConfiguration.get();

    String mapredJobTracker = config.get("mapred.job.tracker");
    Preconditions.checkNotNull(mapredJobTracker);
    Matcher mapredJobTrackerMatcher = JOBTRACKER_PATTERN.matcher(mapredJobTracker);
    Preconditions.checkState(mapredJobTrackerMatcher.find(), "%s didn't match pattern %s",
                             mapredJobTracker,
                             JOBTRACKER_PATTERN.pattern());
    String host = mapredJobTrackerMatcher.group(1);

    String jobtrackerHttpAddress = config.get("mapred.job.tracker.http.address");
    Preconditions.checkNotNull(jobtrackerHttpAddress);
    Matcher jobtrackerHttpAddressMatcher = JOBTRACKER_HTTP_ADDRESS_PATTERN.matcher(jobtrackerHttpAddress);
    Preconditions.checkState(jobtrackerHttpAddressMatcher.find(), "%s didn't match pattern %s",
                             jobtrackerHttpAddress,
                             JOBTRACKER_HTTP_ADDRESS_PATTERN.pattern());
    int port = Integer.parseInt(jobtrackerHttpAddressMatcher.group(1));

    return URI.create("http://" + host + ':' + port);
  }

  @Override
  public Void call() throws InterruptedException, IOException, JobException {

    DeployRemoteJarFile.maybeUploadMyrrixJar(getInstanceID());

    for (Collection<Class<? extends JobStep>> preStepClasses : getPreSchedule()) {
      runSchedule(preStepClasses, 0, false);
    }
    
    int iterationNumber = getStartIteration();
    
    if (Boolean.parseBoolean(System.getProperty("model.als.iterate", "true"))) {
      
      // Normal case: iterate      
      boolean converged = false;
      while (!converged) {
        for (Collection<Class<? extends JobStep>> iterationStepClasses : getIterationSchedule()) {
          runSchedule(iterationStepClasses, iterationNumber, true); // X
        }
        for (Collection<Class<? extends JobStep>> iterationStepClasses : getIterationSchedule()) {
          runSchedule(iterationStepClasses, iterationNumber, false); // Y
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
      for (Collection<Class<? extends JobStep>> iterationStepClasses : getIterationSchedule()) {
        runSchedule(iterationStepClasses, iterationNumber, true); // X
      }
      
    }
    
    for (Collection<Class<? extends JobStep>> postStepClasses : getPostSchedule()) {
      runSchedule(postStepClasses, iterationNumber, false);
    }

    return null;
  }

  @Override
  public void close() {
    // do nothing
  }

  private void runSchedule(Collection<Class<? extends JobStep>> parallelStepClasses,
                           int iteration,
                           boolean computingX) throws JobException, InterruptedException, IOException {

    JobStepConfig config = new JobStepConfig(Namespaces.get().getBucket(),
                                             getInstanceID(),
                                             getGenerationID(),
                                             getLastGenerationID(),
                                             iteration,
                                             computingX);
    String[] args = config.toArgsArray();

    if (parallelStepClasses.size() > 1) {

      Collection<String> stepClassNames = Lists.newArrayListWithCapacity(parallelStepClasses.size());
      for (Class<? extends Tool> stepClass : parallelStepClasses) {
        stepClassNames.add(stepClass.getName());
      }
      String joinedStepClassNames = Joiner.on(',').join(stepClassNames);
      String[] argsPlusSteps = new String[args.length + 1];
      System.arraycopy(args, 0, argsPlusSteps, 0, args.length);
      argsPlusSteps[argsPlusSteps.length - 1] = joinedStepClassNames;
      ParallelStep step = new ParallelStep();
      stateSources.add(step);
      JobStep.run(step, argsPlusSteps);

    } else {

      Class<? extends JobStep> stepClass = parallelStepClasses.iterator().next();
      JobStep step = ClassUtils.loadInstanceOf(stepClass);
      stateSources.add(step);
      JobStep.run(step, args);

    }
  }

}
