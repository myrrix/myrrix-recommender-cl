/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.web;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.lexicalscope.jewel.cli.ArgumentValidationException;
import com.lexicalscope.jewel.cli.CliFactory;
import org.apache.catalina.Context;
import org.apache.catalina.Engine;
import org.apache.catalina.Host;
import org.apache.catalina.Lifecycle;
import org.apache.catalina.LifecycleEvent;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleListener;
import org.apache.catalina.Server;
import org.apache.catalina.authenticator.DigestAuthenticator;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.core.JasperListener;
import org.apache.catalina.core.JreMemoryLeakPreventionListener;
import org.apache.catalina.core.ThreadLocalLeakPreventionListener;
import org.apache.catalina.deploy.ApplicationListener;
import org.apache.catalina.deploy.ErrorPage;
import org.apache.catalina.deploy.LoginConfig;
import org.apache.catalina.deploy.SecurityCollection;
import org.apache.catalina.deploy.SecurityConstraint;
import org.apache.catalina.startup.Tomcat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.batch.PeriodicRunnerConfig;
import net.myrrix.batch.web.servlets.LogServlet;
import net.myrrix.batch.web.servlets.PeriodicRunnerServlet;
import net.myrrix.common.io.IOUtils;
import net.myrrix.common.log.MemoryHandler;
import net.myrrix.common.signal.SignalManager;
import net.myrrix.common.signal.SignalType;
import net.myrrix.store.Namespaces;
import net.myrrix.store.Store;

/**
 * <p>This class will periodically run one generation of the Computation Layer using
 * {@link net.myrrix.batch.GenerationRunner}. It can run after a period of time has elapsed, or an amount of
 * data has been written.</p>
 *
 * <p>Flags available include:</p>
 *
 * <ul>
 *   <li>{@code --instanceID}: Instance ID to operate on within the bucket</li>
 *   <li>{@code --bucket}: Bucket containing data to access for computation</li>
 *   <li>{@code --time}: Time in minutes that will trigger a generation. This may alternatively be specified
 *     with a suffix indicating a different time unit. "5m" means 5 minutes; "2h" means 2 hours;
 *     "1d" means 1 day. Also, two values may be specified, separated by a comma; the first will be
 *     interpreted as the delay before the first scheduled run, and the other will be used as the
 *     delay between subsequent runs. Example: {@code --time 1h,6h} will schedule a run in 1 hour,
 *     and then every 6 hours after. Defaults to 1440 (1 day).</li>
 *   <li>{@code --data}: New data in MB that will trigger a generation. This may alternatively be specified
 *     with a suffix indicating a different unit. "200m" means 200 megabytes; "10g" means 10 gigabytes;
 *     "1t" means 1 terabyte. Defaults to 1000 (1GB)</li>
 *   <li>{@code --recommend}: If set, also computes recommendations for all users and outputs under
 *     {@code recommend/}</li>
 *   <li>{@code --itemSimilarity}: If set, also computes item-item similarity for all items and outputs
 *     under {@code similarItems/}</li>
 *   <li>{@code --forceRun}: If set, force one generation run at startup.</li>
 *   <li>{@code --port}: Port on which to listen for HTTP requests. Defaults to 80. Note that the server must be run
 *   as the root user to access port 80.</li>
 *   <li>{@code --securePort}: Port on which to listen for HTTPS requests. Defaults to 443. Likewise note that
 *   using port 443 requires running as root.</li>
 *   <li>{@code --keystoreFile}: File containing the SSL key to use for HTTPS. Setting this flag
 *   enables HTTPS connections, and so requires that option {@code --keystorePassword} be set.
 *   If not set, will attempt to load a keystore file from the distributed file system,
 *   at {@code sys/keystore.ks}</li>
 *   <li>{@code --keystorePassword}: Password for keystoreFile. Setting this flag enables HTTPS connections.</li>
 *   <li>{@code --userName}: If specified, the user name required to authenticate to the server using
 *   HTTP DIGEST authentication. Requires password to be set.</li>
 *   <li>{@code --password}: Password for HTTP DIGEST authentication. Requires userName to be set.</li>
 *   <li>{@code --consoleOnlyPassword}: Only apply username and password to admin / console pages.</li>
 * </ul>
 *
 * <p>Example:</p>
 *
 * <p>{@code
 * java -jar myrrix-computation-x.y.jar --instanceID 12 --bucket mybucket --time 720 --recommend
 * }</p>
 *
 * @author Sean Owen
 * @since 1.0
 */
public final class Runner implements Callable<Object>, Closeable {

  private static final Logger log = LoggerFactory.getLogger(Runner.class);

  private static final int[] ERROR_PAGE_STATUSES = {
      HttpServletResponse.SC_BAD_REQUEST,
      HttpServletResponse.SC_UNAUTHORIZED,
      HttpServletResponse.SC_NOT_FOUND,
      HttpServletResponse.SC_METHOD_NOT_ALLOWED,
      HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
      HttpServletResponse.SC_SERVICE_UNAVAILABLE,
  };

  private final RunnerConfiguration config;
  private Tomcat tomcat;
  private File noSuchBaseDir;
  private boolean closed;

  public Runner(RunnerConfiguration config) {
    Preconditions.checkNotNull(config);
    this.config = config;
  }

  /**
   * Starts the main loop, which runs indefinitely.
   */
  @Override
  public Object call() throws IOException {

    MemoryHandler.setSensibleLogFormat();
    java.util.logging.Logger.getLogger("").addHandler(new MemoryHandler());

    this.noSuchBaseDir = Files.createTempDir();
    this.noSuchBaseDir.deleteOnExit();

    Tomcat tomcat = new Tomcat();
    Connector connector = makeConnector();
    configureTomcat(tomcat, connector);
    configureEngine(tomcat.getEngine());
    configureServer(tomcat.getServer());
    configureHost(tomcat.getHost());
    Context context = makeContext(tomcat, noSuchBaseDir);

    addServlet(context, new PeriodicRunnerServlet(), "/periodicRunner/*");
    addServlet(context, new index_jspx(), "/index.jspx");
    addServlet(context, new status_jspx(), "/status.jspx");
    addServlet(context, new error_jspx(), "/error.jspx");
    addServlet(context, new LogServlet(), "/log.txt");

    try {
      tomcat.start();
    } catch (LifecycleException le) {
      throw new IOException(le);
    }
    this.tomcat = tomcat;

    return null;
  }

  /**
   * Blocks and waits until the server shuts down.
   */
  public void await() {
    tomcat.getServer().await();
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      closed = true;
      if (tomcat != null) {
        try {
          tomcat.stop();
          tomcat.destroy();
        } catch (LifecycleException le) {
          log.warn("Unexpected error while stopping", le);
        }
        if (!IOUtils.deleteRecursively(noSuchBaseDir)) {
          log.info("Could not delete {}", noSuchBaseDir);
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {

    RunnerConfiguration config;
    try {
      RunnerArgs runnerArgs = CliFactory.parseArguments(RunnerArgs.class, args);
      config = buildConfiguration(runnerArgs);
    } catch (ArgumentValidationException ave) {
      printHelp(ave.getMessage());
      return;
    }

    final Runner runner = new Runner(config);
    runner.call();

    SignalManager.register(new Runnable() {
        @Override
        public void run() {
          runner.close();
        }
      }, SignalType.INT, SignalType.TERM);

    runner.await();
    runner.close();
  }

  private static RunnerConfiguration buildConfiguration(RunnerArgs runnerArgs) {
    RunnerConfiguration config = new RunnerConfiguration();
    PeriodicRunnerConfig periodicRunnerConfig = config.getPeriodicRunnerConfig();

    periodicRunnerConfig.setInstanceID(runnerArgs.getInstanceID());
    periodicRunnerConfig.setBucket(runnerArgs.getBucket());

    periodicRunnerConfig.setDataThresholdMB(parseDataThresholdMB(runnerArgs.getData()));
    int[] initialScheduled = parseInitialAndScheduledTimeDelayMins(runnerArgs.getTime());
    periodicRunnerConfig.setInitialTimeThresholdMin(initialScheduled[0]);
    periodicRunnerConfig.setTimeThresholdMin(initialScheduled[1]);

    periodicRunnerConfig.setRecommend(runnerArgs.isRecommend());
    periodicRunnerConfig.setMakeItemSimilarity(runnerArgs.isItemSimilarity());
    periodicRunnerConfig.setCluster(runnerArgs.isCluster());
    periodicRunnerConfig.setForceRun(runnerArgs.isForceRun());

    config.setPort(runnerArgs.getPort());
    config.setSecurePort(runnerArgs.getSecurePort());
    config.setUserName(runnerArgs.getUserName());
    config.setPassword(runnerArgs.getPassword());
    config.setConsoleOnlyPassword(runnerArgs.isConsoleOnlyPassword());
    config.setKeystoreFile(runnerArgs.getKeystoreFile());
    config.setKeystorePassword(runnerArgs.getKeystorePassword());

    return config;
  }

  /**
   * @return the delay before first run, in minutes, and the delay between subseqent runs, in minutes
   */
  private static int[] parseInitialAndScheduledTimeDelayMins(String timeFlag) {
    int comma = timeFlag.indexOf(',');
    if (comma >= 0) {
      int initialDelay = parseTimeDelayMins(timeFlag.substring(0, comma));
      int scheduledDelay = parseTimeDelayMins(timeFlag.substring(comma + 1));
      return new int[] { initialDelay, scheduledDelay };
    } else {
      int delay = parseTimeDelayMins(timeFlag);
      return new int[] { delay, delay };
    }
  }

  /**
   * @param timeFlag specifies a time in minutes as a number followed by optional unit designator. 'm' means minute,
   *  'h' means hour, 'd' means day. If omitted, minutes is assumed. Example: "2h" means 120 minutes.
   * @return time in minutes
   */
  private static int parseTimeDelayMins(String timeFlag) {
    int lastPosition = timeFlag.length() - 1;
    char units = timeFlag.charAt(lastPosition);
    if (Character.isDigit(units)) {
      return Integer.parseInt(timeFlag);
    }
    int rawValue = Integer.parseInt(timeFlag.substring(0, lastPosition));
    TimeUnit unit;
    switch (units) {
      case 'm':
        unit = TimeUnit.MINUTES;
        break;
      case 'h':
        unit = TimeUnit.HOURS;
        break;
      case 'd':
        unit = TimeUnit.DAYS;
        break;
      default:
        throw new IllegalArgumentException(timeFlag);
    }
    return (int) TimeUnit.MINUTES.convert(rawValue, unit);
  }

  private static int parseDataThresholdMB(String dataFlag) {
    int lastPosition = dataFlag.length() - 1;
    char units = dataFlag.charAt(lastPosition);
    if (Character.isDigit(units)) {
      return Integer.parseInt(dataFlag);
    }
    int rawValue = Integer.parseInt(dataFlag.substring(0, lastPosition));
    switch (units) {
      case 'm':
        return rawValue;
      case 'g':
        return 1000 * rawValue;
      case 't':
        return 1000000 * rawValue;
      default:
        throw new IllegalArgumentException(dataFlag);
    }
  }

  private static void printHelp(String message) {
    System.out.println();
    System.out.println("Myrrix Computation Layer. Copyright Myrrix Ltd, except for included ");
    System.out.println("third-party open source software. Full details of licensing at http://myrrix.com/legal/");
    System.out.println();
    if (message != null) {
      System.out.println(message);
      System.out.println();
    }
  }

  private void configureTomcat(Tomcat tomcat, Connector connector) {
    tomcat.setBaseDir(noSuchBaseDir.getAbsolutePath());
    tomcat.setConnector(connector);
    tomcat.getService().addConnector(connector);
  }

  private void configureEngine(Engine engine) {
    String userName = config.getUserName();
    String password = config.getPassword();
    if (userName != null && password != null) {
      InMemoryRealm realm = new InMemoryRealm();
      realm.addUser(userName, password);
      engine.setRealm(realm);
    }
  }

  private static void configureServer(Server server) {
    //server.addLifecycleListener(new SecurityListener());
    //server.addLifecycleListener(new AprLifecycleListener());
    LifecycleListener jasperListener = new JasperListener();
    server.addLifecycleListener(jasperListener);
    jasperListener.lifecycleEvent(new LifecycleEvent(server, Lifecycle.BEFORE_INIT_EVENT, null));
    server.addLifecycleListener(new JreMemoryLeakPreventionListener());
    //server.addLifecycleListener(new GlobalResourcesLifecycleListener());
    server.addLifecycleListener(new ThreadLocalLeakPreventionListener());
  }

  private static void configureHost(Host host) {
    host.setAutoDeploy(false);
  }

  private Connector makeConnector() throws IOException {
    Connector connector = new Connector("org.apache.coyote.http11.Http11NioProtocol");
    File keystoreFile = config.getKeystoreFile();
    String keystorePassword = config.getKeystorePassword();
    if (keystoreFile == null && keystorePassword == null) {
      // HTTP connector
      connector.setPort(config.getPort());
      connector.setSecure(false);
      connector.setScheme("http");

    } else {

      if (keystoreFile == null || !keystoreFile.exists()) {
        log.info("Keystore file not found; trying to load remote keystore file if applicable");
        String instanceID = config.getPeriodicRunnerConfig().getInstanceID();
        keystoreFile = getResourceAsFile(Namespaces.getKeystoreFilePrefix(instanceID));
        if (keystoreFile == null) {
          throw new FileNotFoundException();
        }
      }

      // HTTPS connector
      connector.setPort(config.getSecurePort());
      connector.setSecure(true);
      connector.setScheme("https");
      connector.setAttribute("SSLEnabled", "true");
      String protocol = chooseSSLProtocol("TLSv1.1", "TLSv1");
      if (protocol != null) {
        connector.setAttribute("sslProtocol", protocol);
      }
      connector.setAttribute("keystoreFile", keystoreFile.getAbsoluteFile());
      connector.setAttribute("keystorePass", keystorePassword);
    }

    // Keep quiet about the server type
    connector.setXpoweredBy(false);
    connector.setAttribute("server", "Myrrix");

    return connector;
  }
  
  private static String chooseSSLProtocol(String... protocols) {
    for (String protocol : protocols) {
      try {
        SSLContext.getInstance(protocol);
        return protocol;
      } catch (NoSuchAlgorithmException ignored) {
        // continue
      }
    }
    return null;
  }

  private static File getResourceAsFile(String resource) throws IOException {
    Preconditions.checkNotNull(resource);

    String suffix;
    int dot = resource.lastIndexOf('.');
    if (dot >= 0) {
      suffix = resource.substring(dot);
    } else {
      suffix = ".tmp";
    }
    File tempFile = File.createTempFile("myrrix-", suffix);
    tempFile.deleteOnExit();

    try {
      IOUtils.copyURLToFile(new URL(resource), tempFile);
    } catch (MalformedURLException ignored) {
      Store store = Store.get();
      store.download(resource, tempFile);
    }
    return tempFile;
  }

  private Context makeContext(Tomcat tomcat, File noSuchBaseDir) throws IOException {

    File contextPath = new File(noSuchBaseDir, "context");
    if (!contextPath.mkdirs()) {
      throw new IOException("Could not create " + contextPath);
    }

    Context context = tomcat.addContext("", contextPath.getAbsolutePath());
    context.addApplicationListener(new ApplicationListener(InitListener.class.getName(), false));
    context.setWebappVersion("3.0");
    context.addWelcomeFile("index.jspx");
    addErrorPages(context);

    ServletContext servletContext = context.getServletContext();
    servletContext.setAttribute(InitListener.PERIODIC_RUNNER_CONFIG_KEY, config.getPeriodicRunnerConfig());

    boolean needHTTPS = config.getKeystoreFile() != null;
    boolean needAuthentication = config.getUserName() != null;

    if (needHTTPS || needAuthentication) {

      SecurityCollection securityCollection = new SecurityCollection("Protected Resources");
      if (config.isConsoleOnlyPassword()) {
        securityCollection.addPattern("/index.jspx");
      } else {
        securityCollection.addPattern("/*");
      }
      SecurityConstraint securityConstraint = new SecurityConstraint();
      securityConstraint.addCollection(securityCollection);

      if (needHTTPS) {
        securityConstraint.setUserConstraint("CONFIDENTIAL");
      }

      if (needAuthentication) {

        LoginConfig loginConfig = new LoginConfig();
        loginConfig.setAuthMethod("DIGEST");
        loginConfig.setRealmName(InMemoryRealm.NAME);
        context.setLoginConfig(loginConfig);

        securityConstraint.addAuthRole(InMemoryRealm.AUTH_ROLE);

        context.addSecurityRole(InMemoryRealm.AUTH_ROLE);
        context.getPipeline().addValve(new DigestAuthenticator());
      }

      context.addConstraint(securityConstraint);
    }

    context.setCookies(false);

    return context;
  }

  private static void addServlet(Context context, Servlet servlet, String path) {
    String name = servlet.getClass().getSimpleName();
    Tomcat.addServlet(context, name, servlet).setLoadOnStartup(1);
    context.addServletMapping(path, name);
  }

  private static void addErrorPages(Context context) {
    for (int errorCode : ERROR_PAGE_STATUSES) {
      ErrorPage errorPage = new ErrorPage();
      errorPage.setErrorCode(errorCode);
      errorPage.setLocation("/error.jspx");
      context.addErrorPage(errorPage);
    }
    ErrorPage errorPage = new ErrorPage();
    errorPage.setExceptionType(Throwable.class.getName());
    errorPage.setLocation("/error.jspx");
    context.addErrorPage(errorPage);
  }

}
