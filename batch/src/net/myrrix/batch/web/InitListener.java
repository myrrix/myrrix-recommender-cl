/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.web;

import java.util.logging.Handler;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.batch.PeriodicRunner;
import net.myrrix.batch.PeriodicRunnerConfig;
import net.myrrix.common.log.MemoryHandler;
import net.myrrix.store.Namespaces;

/**
 * <p>This servlet lifecycle listener makes sure that the shared resources are
 * initialized at startup, along with related objects, and shut down when the container is destroyed.</p>
 *
 * @author Sean Owen
 * @since 1.0
 */
public final class InitListener implements ServletContextListener {

  private static final Logger log = LoggerFactory.getLogger(InitListener.class);

  private static final String KEY_PREFIX = InitListener.class.getName();
  public static final String LOG_HANDLER = KEY_PREFIX + ".LOG_HANDLER";
  public static final String PERIODIC_RUNNER_CONFIG_KEY = KEY_PREFIX + ".PERIODIC_RUNNER_CONFIG";
  public static final String PERIODIC_RUNNER_KEY = KEY_PREFIX + ".PERIODIC_RUNNER";

  private PeriodicRunner runner;

  @Override
  public void contextInitialized(ServletContextEvent event) {
    log.info("Initializing Myrrix in servlet context...");
    ServletContext context = event.getServletContext();

    MemoryHandler.setSensibleLogFormat();
    Handler logHandler = null;
    for (Handler handler : java.util.logging.Logger.getLogger("").getHandlers()) {
      if (handler instanceof MemoryHandler) {
        logHandler = handler;
        break;
      }
    }
    if (logHandler == null) {
      // Not previously configured by command line, make a new one
      logHandler = new MemoryHandler();
      java.util.logging.Logger.getLogger("").addHandler(logHandler);
    }
    context.setAttribute(LOG_HANDLER, logHandler);

    PeriodicRunnerConfig config =
        (PeriodicRunnerConfig) context.getAttribute(PERIODIC_RUNNER_CONFIG_KEY);

    Namespaces.setGlobalBucket(config.getBucket());

    log.info("Starting PeriodicRunner...");
    runner = new PeriodicRunner(config);
    context.setAttribute(PERIODIC_RUNNER_KEY, runner);

    log.info("Myrrix is initialized");
  }

  @Override
  public void contextDestroyed(ServletContextEvent servletContextEvent) {
    log.info("Uninitializing Myrrix in servlet context...");
    if (runner != null) {
      runner.close();
    }
    log.info("Myrrix is uninitialized");
  }

}
