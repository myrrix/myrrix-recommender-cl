/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.web.servlets;

import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Charsets;
import com.google.common.net.MediaType;

import net.myrrix.common.log.MemoryHandler;
import net.myrrix.batch.web.InitListener;

/**
 * Prints recent log messages to the response.
 *
 * @author Sean Owen
 * @since 1.0
 */
public final class LogServlet extends HttpServlet {

  private static final String CONTENT_TYPE = MediaType.PLAIN_TEXT_UTF_8.withoutParameters().toString();

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    MemoryHandler logHandler = (MemoryHandler) getServletContext().getAttribute(InitListener.LOG_HANDLER);
    response.setContentType(CONTENT_TYPE);
    response.setCharacterEncoding(Charsets.UTF_8.name());
    Writer out = response.getWriter();
    Iterable<String> lines = logHandler.getLogLines();
    synchronized (lines) {
      for (String line : lines) {
        out.write(line); // Already has newline
      }
    }
  }

}
