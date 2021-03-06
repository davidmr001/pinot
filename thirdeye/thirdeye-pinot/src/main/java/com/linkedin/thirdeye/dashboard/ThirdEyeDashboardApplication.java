package com.linkedin.thirdeye.dashboard;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.common.BaseThirdEyeApplication;
import com.linkedin.thirdeye.dashboard.resources.CacheResource;
import com.linkedin.thirdeye.dashboard.resources.DashboardResource;
import com.linkedin.thirdeye.detector.resources.AnomalyResultResource;

import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;

public class ThirdEyeDashboardApplication
    extends BaseThirdEyeApplication<ThirdEyeDashboardConfiguration> {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeDashboardApplication.class);

  public static final String WEBAPP_CONFIG = "/webapp-config";

  public ThirdEyeDashboardApplication() {

  }

  @Override
  public String getName() {
    return "Thirdeye Dashboard";
  }

  @Override
  public void initialize(Bootstrap<ThirdEyeDashboardConfiguration> bootstrap) {
    bootstrap.addBundle(new ViewBundle());
    bootstrap.addBundle(new HelperBundle());
    bootstrap.addBundle(hibernateBundle);
    bootstrap.addBundle(new AssetsBundle("/assets", "/assets"));
    bootstrap.addBundle(new AssetsBundle("/assets/css", "/assets/css", null, "css"));
    bootstrap.addBundle(new AssetsBundle("/assets/js", "/assets/js", null, "js"));
    bootstrap.addBundle(new AssetsBundle("/assets/lib", "/assets/lib", null, "lib"));
    bootstrap.addBundle(new AssetsBundle("/assets/img", "/assets/img", null, "img"));
    bootstrap.addBundle(new AssetsBundle("/assets/data", "/assets/data", null, "data"));

  }

  @Override
  public void run(ThirdEyeDashboardConfiguration config, Environment env) throws Exception {
    super.initDetectorRelatedDAO();

    try {
      ThirdEyeCacheRegistry.initializeWebappCaches(config);
    } catch (Exception e) {
      LOG.error("Exception while loading caches", e);
    }

    env.jersey().register(new DashboardResource(BaseThirdEyeApplication.getDashboardConfigDAO(config)));
    env.jersey().register(new AnomalyResultResource(anomalyResultDAO));
    env.jersey().register(new CacheResource());
  }

  public static void main(String[] args) throws Exception {
    String thirdEyeConfigDir = args[0];
    System.setProperty("dw.rootDir", thirdEyeConfigDir);
    String dashboardApplicationConfigFile = thirdEyeConfigDir + "/" + "dashboard.yml";
    new ThirdEyeDashboardApplication().run(new String[] {
        "server", dashboardApplicationConfigFile
    });
  }
}
