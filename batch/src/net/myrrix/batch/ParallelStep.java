/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.common.ClassUtils;
import net.myrrix.store.MyrrixConfiguration;

/**
 * A step which actually runs several {@link JobStep}s in parallel.
 *
 * @author Sean Owen
 * @since 1.0
 */
public final class ParallelStep extends Configured implements Tool, HasState {

  private static final Logger log = LoggerFactory.getLogger(ParallelStep.class);
  private static final Splitter COMMA = Splitter.on(',');

  private final Collection<JobStep> steps;

  public ParallelStep() {
    steps = Lists.newCopyOnWriteArrayList();
  }

  /**
   * @return result of {@code getStepStates()} from this step's parallel component steps.
   */
  @Override
  public Collection<MyrrixStepState> getStepStates() throws IOException {
    Collection<MyrrixStepState> states = Lists.newArrayList();
    for (JobStep step : steps) {
      states.addAll(step.getStepStates());
    }
    return states;
  }

  /**
   * @param args will be passed through to the parallel steps, except for the last. The last is a
   * comma-separated list of class names of steps to execute in parallel.
   */
  @Override
  public int run(final String[] args) throws JobException, IOException, InterruptedException {

    Collection<Callable<Void>> runnables = Lists.newArrayList();
    // all but last arg are passed through
    for (String stepClassName : COMMA.split(args[args.length - 1])) {
      final JobStep step = ClassUtils.loadInstanceOf(stepClassName, JobStep.class);
      steps.add(step);
      runnables.add(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          log.info("Running step {}", step.getClass().getSimpleName());
          ToolRunner.run(getConf(), step, args);
          return null;
        }
      });
    }

    ExecutorService executor = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat("ParallelStep-%d").build());
    Iterable<Future<Void>> futures;
    try {
      futures = executor.invokeAll(runnables);
    } finally {
      // shutdown() now rather than shutdownNow() later; this may let some parallel jobs finish
      executor.shutdown();
    }
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof InterruptedException) {
          log.warn("Interrupted");
          throw (InterruptedException) cause;
        }
        if (cause instanceof IOException) {
          log.warn("Unexpected exception while running step", cause);
          throw (IOException) cause;
        }
        if (cause instanceof JobException) {
          log.warn("Unexpected exception while running step", cause);
          throw (JobException) cause;
        }
        log.error("Unexpected exception while running step", cause);
        throw new JobException(cause);
      }
    }

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(MyrrixConfiguration.get(), new ParallelStep(), args);
  }

}
