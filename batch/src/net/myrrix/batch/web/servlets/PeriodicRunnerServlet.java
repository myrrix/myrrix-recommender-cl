/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.web.servlets;

import java.io.IOException;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.batch.PeriodicRunner;
import net.myrrix.batch.web.InitListener;

/**
 * Manages the {@link PeriodicRunner} in the {@link ServletContext}.
 *
 * @author Sean Owen
 * @since 1.0
 */
public final class PeriodicRunnerServlet extends HttpServlet {

  private static final Logger log = LoggerFactory.getLogger(PeriodicRunnerServlet.class);

  private static final String PREFIX = "/periodicRunner/";

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {

    ServletContext context = getServletContext();
    PeriodicRunner runner = (PeriodicRunner) context.getAttribute(InitListener.PERIODIC_RUNNER_KEY);

    String requestURI = request.getRequestURI();
    Preconditions.checkArgument(requestURI.startsWith(PREFIX), "Bad request URI: %s", requestURI);
    String command = requestURI.substring(PREFIX.length());

    if ("forceRun".equals(command)) {
      log.info("Forcing run of PeriodicRunner");
      runner.forceRun();
    } else if ("interrupt".equals(command)) {
      log.info("Interrupting PeriodicRunner");
      runner.interrupt();
    } else {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unknown command");
      return;
    }

    response.sendRedirect("/index.jspx");
  }

}
