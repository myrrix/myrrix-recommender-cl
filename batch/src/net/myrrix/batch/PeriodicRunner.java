/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.common.parallel.ExecutorUtils;
import net.myrrix.common.ReloadingReference;
import net.myrrix.store.Namespaces;
import net.myrrix.store.Store;
import net.myrrix.store.StoreUtils;

/**
 * <p>This class will periodically run one generation of the Computation Layer using {@link GenerationRunner}.
 * It can run after a period of time has elapsed, or an amount of data has been written.</p>
 *
 * @author Sean Owen
 * @since 1.0
 * @see GenerationRunner
 */
public final class PeriodicRunner implements Runnable, Closeable {
  // Implement Runnable, not Callable, for ScheduledExecutor

  private static final Logger log = LoggerFactory.getLogger(PeriodicRunner.class);

  private static final Splitter ON_DELIMITER = Splitter.on('/');
  private static final long CHECK_INTERVAL_MINS = 1;

  private final PeriodicRunnerConfig config;
  private long lastUpdateTime;
  private long nextScheduledRunTime;
  private boolean forceRun;
  private boolean running;
  private final Collection<GenerationRunner> allGenerationRunners;
  private final ScheduledExecutorService executor;
  private Future<?> future;
  private final ReloadingReference<PeriodicRunnerState> cachedState;

  public PeriodicRunner(PeriodicRunnerConfig config) {

    Preconditions.checkNotNull(config);
    this.config = config;

    lastUpdateTime = System.currentTimeMillis();
    int threshold = config.getInitialTimeThresholdMin();
    if (threshold == PeriodicRunnerConfig.NO_THRESHOLD) {
      nextScheduledRunTime = Long.MAX_VALUE;
    } else {
      nextScheduledRunTime = lastUpdateTime + TimeUnit.MILLISECONDS.convert(threshold, TimeUnit.MINUTES);
    }

    forceRun = config.isForceRun();
    running = false;

    this.allGenerationRunners = Lists.newCopyOnWriteArrayList();

    executor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("PeriodicRunner-%d").build());
    future = executor.scheduleWithFixedDelay(this, 0L, CHECK_INTERVAL_MINS, TimeUnit.MINUTES);

    this.cachedState = new ReloadingReference<PeriodicRunnerState>(new Callable<PeriodicRunnerState>() {
      @Override
      public PeriodicRunnerState call() throws IOException {
        return doGetState();
      }
    }, 15, TimeUnit.SECONDS);
  }

  public PeriodicRunnerConfig getConfig() {
    return config;
  }

  public PeriodicRunnerState getState() {
    return cachedState.get(3, TimeUnit.SECONDS);
  }

  private PeriodicRunnerState doGetState() throws IOException {

    List<GenerationState> states = Lists.newArrayListWithCapacity(allGenerationRunners.size());
    for (GenerationRunner runner : allGenerationRunners) {
      GenerationState runnerSate = runner.getState();
      if (runnerSate != null) {
        states.add(runnerSate);
      }
    }
    Collections.reverse(states);
    Date nextScheduledRunTimeDate = nextScheduledRunTime == Long.MAX_VALUE ? null : new Date(nextScheduledRunTime);

    List<String> mostRecentGenerations = StoreUtils.listGenerationsForInstance(config.getInstanceID());
    int currentGenerationMB = dataInCurrentGenerationMB(mostRecentGenerations);

    return new PeriodicRunnerState(states, running, nextScheduledRunTimeDate, currentGenerationMB);
  }

  public void forceRun() {
    forceRun = true;
    future.cancel(true);
    future = executor.scheduleWithFixedDelay(this, 0L, CHECK_INTERVAL_MINS, TimeUnit.MINUTES);
  }

  public void interrupt() {
    future.cancel(true);
    future = executor.scheduleWithFixedDelay(this, CHECK_INTERVAL_MINS, CHECK_INTERVAL_MINS, TimeUnit.MINUTES);
  }

  @Override
  public void close() {
    ExecutorUtils.shutdownNowAndAwait(executor);
    future.cancel(true);
  }

  @Override
  public void run()  {

    try {
      running = true;
      long now = System.currentTimeMillis();

      boolean timeThresholdExceeded = now >= nextScheduledRunTime;

      List<String> mostRecentGenerations = StoreUtils.listGenerationsForInstance(config.getInstanceID());

      if (mostRecentGenerations.isEmpty()) {
        log.info("Forcing run -- no generations yet");
        forceRun = true;
      }

      int currentGenerationMB = dataInCurrentGenerationMB(mostRecentGenerations);
      int dataThresholdMB = config.getDataThresholdMB();
      boolean dataThresholdExceeded =
          dataThresholdMB != PeriodicRunnerConfig.NO_THRESHOLD && currentGenerationMB >= dataThresholdMB;

      if (forceRun) {
        log.info("Forcing run");
      }
      if (timeThresholdExceeded) {
        log.info("Running new generation due to elapsed time: {} minutes",
                 TimeUnit.MINUTES.convert(now - lastUpdateTime, TimeUnit.MILLISECONDS));
      }
      if (dataThresholdExceeded) {
        log.info("Running new generation due to data written: {}MB", currentGenerationMB);
      }

      if (forceRun || timeThresholdExceeded || dataThresholdExceeded) {
        forceRun = false;
        GenerationRunner newGenerationRunner = new GenerationRunner(config);
        allGenerationRunners.add(newGenerationRunner);

        // If time threshold was exceeded, pretend we started the run at the scheduled time. We noticed
        // up to a minute later, but conceptually it started then. Otherwise the start time is
        // conceptually now (due to data)
        if (now > nextScheduledRunTime) {
          lastUpdateTime = nextScheduledRunTime;
        } else {
          lastUpdateTime = now;
        }
        long msBetweenRuns = TimeUnit.MILLISECONDS.convert(config.getTimeThresholdMin(), TimeUnit.MINUTES);
        nextScheduledRunTime = lastUpdateTime + msBetweenRuns;

        newGenerationRunner.call();

        // If we ran over time limit, schedule will run again immediately. For bookkeeping correctness,
        // move back the scheduled run time to now, to reflect the fact that it was preempted.
        long endTime = System.currentTimeMillis();
        if (endTime > nextScheduledRunTime) {
          nextScheduledRunTime = endTime;
        }
      }

    } catch (InterruptedException ignored) {
      log.warn("Interrupted");
    } catch (IOException ioe) {
      log.warn("Unexpected error in execution", ioe);
    } catch (JobException je) {
      log.warn("Unexpected error in execution", je);
    } catch (Throwable t) {
      log.error("Unexpected error in execution", t);
    } finally {
      running = false;
    }
  }

  private int dataInCurrentGenerationMB(List<String> mostRecentGenerations) throws IOException {
    if (mostRecentGenerations.isEmpty()) {
      return 0;
    }
    CharSequence currentGeneration = mostRecentGenerations.get(mostRecentGenerations.size() - 1);
    long currentGenerationID = parseGenerationFromPrefix(currentGeneration);

    Store store = Store.get();
    long totalSizeBytes = 0L;
    for (String filePrefix :
         store.list(Namespaces.getInboundPrefix(config.getInstanceID(), currentGenerationID), true)) {
      totalSizeBytes += store.getSize(filePrefix);
    }
    return (int) (totalSizeBytes / 1000000);
  }

  private static long parseGenerationFromPrefix(CharSequence prefix) {
    Iterator<String> tokens = ON_DELIMITER.split(prefix).iterator();
    tokens.next();
    return Long.parseLong(tokens.next());
  }

}
