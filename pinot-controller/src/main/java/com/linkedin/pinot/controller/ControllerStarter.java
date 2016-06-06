/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.metrics.MetricsHelper;
import com.linkedin.pinot.common.metrics.ValidationMetrics;
import com.linkedin.pinot.controller.api.ControllerRestApplication;
import com.linkedin.pinot.controller.helix.SegmentStatusChecker;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.realtime.PinotRealtimeSegmentManager;
import com.linkedin.pinot.controller.helix.core.retention.RetentionManager;
import com.linkedin.pinot.controller.validation.ValidationManager;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.helix.PreConnectCallback;
import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.data.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControllerStarter {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerStarter.class);
  private static final String MetricsRegistryName = "pinot.controller.metrics";
  private final ControllerConf config;

  private final Component component;
  private final Application controllerRestApp;
  private final PinotHelixResourceManager helixResourceManager;
  private final RetentionManager retentionManager;
  private final ValidationManager validationManager;
  private final MetricsRegistry _metricsRegistry;
  private final PinotRealtimeSegmentManager realtimeSegmentsManager;
  private final SegmentStatusChecker segmentStatusChecker;
  private final ExecutorService executorService;

  public ControllerStarter(ControllerConf conf) {
    config = conf;
    component = new Component();
    controllerRestApp = new ControllerRestApplication(config.getQueryConsole());
    helixResourceManager = new PinotHelixResourceManager(config);
    retentionManager = new RetentionManager(helixResourceManager, config.getRetentionControllerFrequencyInSeconds());
    _metricsRegistry = new MetricsRegistry();
    ValidationMetrics validationMetrics = new ValidationMetrics(_metricsRegistry);
    validationManager = new ValidationManager(validationMetrics, helixResourceManager, config);
    realtimeSegmentsManager = new PinotRealtimeSegmentManager(helixResourceManager);
    segmentStatusChecker = new SegmentStatusChecker(helixResourceManager, config);
    executorService = Executors.newCachedThreadPool();
  }

  public PinotHelixResourceManager getHelixResourceManager() {
    return helixResourceManager;
  }

  public void start() {
    Utils.logVersions();

    component.getServers().add(Protocol.HTTP, Integer.parseInt(config.getControllerPort()));
    component.getClients().add(Protocol.FILE);
    component.getClients().add(Protocol.JAR);
    component.getClients().add(Protocol.WAR);

    final Context applicationContext = component.getContext().createChildContext();

    LOGGER.info("injecting conf and resource manager to the api context");
    applicationContext.getAttributes().put(ControllerConf.class.toString(), config);
    applicationContext.getAttributes().put(PinotHelixResourceManager.class.toString(), helixResourceManager);
    applicationContext.getAttributes().put(HttpConnectionManager.class.toString(), new MultiThreadedHttpConnectionManager());
    applicationContext.getAttributes().put(Executor.class.toString(), executorService);

    controllerRestApp.setContext(applicationContext);

    component.getDefaultHost().attach(controllerRestApp);

    MetricsHelper.initializeMetrics(config.subset("pinot.controller.metrics"));
    MetricsHelper.registerMetricsRegistry(_metricsRegistry);
    final ControllerMetrics controllerMetrics = new ControllerMetrics(_metricsRegistry);

    try {
      LOGGER.info("starting pinot helix resource manager");
      helixResourceManager.start();
      LOGGER.info("starting api component");
      component.start();
      LOGGER.info("starting retention manager");
      retentionManager.start();
      LOGGER.info("starting validation manager");
      validationManager.start();
      LOGGER.info("starting realtime segments manager");
      realtimeSegmentsManager.start(controllerMetrics);
      LOGGER.info("starting segments status manager");
      segmentStatusChecker.start(controllerMetrics);
    } catch (final Exception e) {
      LOGGER.error("Caught exception while starting controller", e);
      Utils.rethrowException(e);
      throw new AssertionError("Should not reach this");
    }

    controllerMetrics.addCallbackGauge(
            "helix.connected",
            new Callable<Long>() {
              @Override
              public Long call() throws Exception {
                return helixResourceManager.getHelixZkManager().isConnected() ? 1L : 0L;
              }
            });

    controllerMetrics.addCallbackGauge(
        "helix.leader", new Callable<Long>() {
              @Override
              public Long call() throws Exception {
                return helixResourceManager.getHelixZkManager().isLeader() ? 1L : 0L;
              }
            });

    helixResourceManager.getHelixZkManager().addPreConnectCallback(new PreConnectCallback() {
      @Override
      public void onPreConnect() {
        controllerMetrics.addMeteredGlobalValue(ControllerMeter.HELIX_ZOOKEEPER_RECONNECTS, 1L);
      }
    });
    controllerMetrics.initializeGlobalMeters();
    ControllerRestApplication.setControllerMetrics(controllerMetrics);
  }

  public void stop() {
    try {
      LOGGER.info("stopping validation manager");
      validationManager.stop();

      LOGGER.info("stopping retention manager");
      retentionManager.stop();

      LOGGER.info("stopping api component");
      component.stop();

      LOGGER.info("stopping realtime segments manager");
      realtimeSegmentsManager.stop();

      LOGGER.info("stopping resource manager");
      helixResourceManager.stop();

      LOGGER.info("stopping segments status manager");
      segmentStatusChecker.stop();

      executorService.shutdownNow();
    } catch (final Exception e) {
      LOGGER.error("Caught exception", e);
    }
  }

  public static ControllerStarter startDefault() {
    return startDefault(null);
  }

  public static ControllerStarter startDefault(File webappPath) {
    final ControllerConf conf = new ControllerConf();
    conf.setControllerHost("localhost");
    conf.setControllerPort("9000");
    conf.setDataDir("/tmp/PinotController");
    conf.setZkStr("localhost:2122");
    conf.setHelixClusterName("quickstart");
    if (webappPath == null) {
      String path = ControllerStarter.class.getClassLoader().getResource("webapp").getFile();
      if (!path.startsWith("file://")) {
        path = "file://" + path;
      }
      conf.setQueryConsolePath(path);
    } else {
      conf.setQueryConsolePath("file://" + webappPath.getAbsolutePath());
    }

    conf.setControllerVipHost("localhost");
    conf.setControllerVipProtocol("http");
    conf.setRetentionControllerFrequencyInSeconds(3600 * 6);
    conf.setValidationControllerFrequencyInSeconds(3600);
    conf.setStatusCheckerFrequencyInSeconds(5*60);
    conf.setTenantIsolationEnabled(true);
    final ControllerStarter starter = new ControllerStarter(conf);

    starter.start();
    return starter;
  }

  public static void main(String[] args) throws InterruptedException {
    startDefault();
  }
}
