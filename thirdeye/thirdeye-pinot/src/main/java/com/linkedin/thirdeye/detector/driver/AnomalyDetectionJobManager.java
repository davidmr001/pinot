package com.linkedin.thirdeye.detector.driver;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.SessionFactory;
import org.joda.time.DateTime;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesHandler;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponseConverter;
import com.linkedin.thirdeye.detector.api.AnomalyFunctionSpec;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionRelationDAO;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyResultDAO;
import com.linkedin.thirdeye.detector.driver.TestAnomalyApplication.TestType;
import com.linkedin.thirdeye.detector.function.AnomalyFunction;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

public class AnomalyDetectionJobManager {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetectionJobManager.class);
  private final Scheduler quartzScheduler;
  // private final ThirdEyeClient thirdEyeClient;
  private final TimeSeriesHandler timeSeriesHandler;
  private final TimeSeriesResponseConverter timeSeriesResponseConverter;
  private final AnomalyFunctionSpecDAO specDAO;
  private final AnomalyFunctionRelationDAO relationDAO;
  private final AnomalyResultDAO resultDAO;
  private final SessionFactory sessionFactory;
  private final Object sync;
  private final Map<Long, String> scheduledJobKeys;
  private final MetricRegistry metricRegistry;
  private final AnomalyFunctionFactory anomalyFunctionFactory;
  private final FailureEmailConfiguration failureEmailConfig;

  private static final ObjectMapper reader = new ObjectMapper(new YAMLFactory());

  public AnomalyDetectionJobManager(Scheduler quartzScheduler, TimeSeriesHandler timeSeriesHandler,
      TimeSeriesResponseConverter timeSeriesResponseConverter, AnomalyFunctionSpecDAO specDAO,
      AnomalyFunctionRelationDAO relationDAO, AnomalyResultDAO resultDAO,
      SessionFactory sessionFactory, MetricRegistry metricRegistry,
      AnomalyFunctionFactory anomalyFunctionFactory, FailureEmailConfiguration failureEmailConfig) {
    this.quartzScheduler = quartzScheduler;
    // this.thirdEyeClient = thirdEyeClient;
    this.timeSeriesHandler = timeSeriesHandler;
    this.timeSeriesResponseConverter = timeSeriesResponseConverter;
    this.specDAO = specDAO;
    this.relationDAO = relationDAO;
    this.resultDAO = resultDAO;
    this.sessionFactory = sessionFactory;
    this.metricRegistry = metricRegistry;
    this.sync = new Object();
    this.scheduledJobKeys = new HashMap<>();
    this.anomalyFunctionFactory = anomalyFunctionFactory;
    this.failureEmailConfig = failureEmailConfig;
  }

  public List<Long> getActiveJobs() {
    synchronized (sync) {
      List<Long> jobs = new ArrayList<>(scheduledJobKeys.keySet());
      Collections.sort(jobs);
      return jobs;
    }
  }

  public void runAdHoc(Long id, String windowStartIsoString, String windowEndIsoString)
      throws Exception {
    synchronized (sync) {
      AnomalyFunctionSpec spec = specDAO.findById(id);
      if (spec == null) {
        throw new IllegalArgumentException("No function with id " + id);
      }
      AnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(spec);

      String triggerKey = String.format("ad_hoc_anomaly_function_trigger_%d", spec.getId());
      Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey).startNow().build();

      String jobKey = String.format("ad_hoc_anomaly_function_job_%d", spec.getId());
      buildAndScheduleJob(jobKey, trigger, anomalyFunction, spec, windowStartIsoString,
          windowEndIsoString);
    }
  }

  /**
   * @param jobKey
   * @param trigger
   * @param anomalyFunction
   * @param spec
   * @param windowStartIsoString
   * @param windowEndIsoString
   * @throws SchedulerException
   */
  private void buildAndScheduleJob(String jobKey, Trigger trigger, AnomalyFunction anomalyFunction,
      AnomalyFunctionSpec spec, String windowStartIsoString, String windowEndIsoString)
      throws SchedulerException {
    JobDetail job = JobBuilder.newJob(AnomalyDetectionJob.class).withIdentity(jobKey).build();

    job.getJobDataMap().put(AnomalyDetectionJob.FUNCTION, anomalyFunction);
    // job.getJobDataMap().put(AnomalyDetectionJob.CLIENT, thirdEyeClient);
    job.getJobDataMap().put(AnomalyDetectionJob.TIME_SERIES_HANDLER, timeSeriesHandler);
    job.getJobDataMap().put(AnomalyDetectionJob.TIME_SERIES_RESPONSE_CONVERTER,
        timeSeriesResponseConverter);
    job.getJobDataMap().put(AnomalyDetectionJob.WINDOW_START, windowStartIsoString);
    job.getJobDataMap().put(AnomalyDetectionJob.WINDOW_END, windowEndIsoString);
    job.getJobDataMap().put(AnomalyDetectionJob.RESULT_DAO, resultDAO);
    job.getJobDataMap().put(AnomalyDetectionJob.SESSION_FACTORY, sessionFactory);
    job.getJobDataMap().put(AnomalyDetectionJob.METRIC_REGISTRY, metricRegistry);
    job.getJobDataMap().put(AnomalyDetectionJob.RELATION_DAO, relationDAO);

    job.getJobDataMap().put(FailureEmailConfiguration.FAILURE_EMAIL_CONFIG_KEY, failureEmailConfig);

    quartzScheduler.scheduleJob(job, trigger);

    LOG.info("Started {}: {}", jobKey, spec);
  }

  public void start(Long id) throws Exception {
    synchronized (sync) {
      AnomalyFunctionSpec spec = specDAO.findById(id);
      if (spec == null) {
        throw new IllegalArgumentException("No function with id " + id);
      }
      AnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(spec);

      String triggerKey = String.format("scheduled_anomaly_function_trigger_%d", spec.getId());
      CronTrigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey)
          .withSchedule(CronScheduleBuilder.cronSchedule(spec.getCron())).build();

      String jobKey = String.format("scheduled_anomaly_function_job_%d", spec.getId());
      scheduledJobKeys.put(id, jobKey);

      buildAndScheduleJob(jobKey, trigger, anomalyFunction, spec, null, null); // use schedule time
                                                                               // to determine
                                                                               // start/end
    }
  }

  public void stop(Long id) throws Exception {
    synchronized (sync) {
      String jobKey = scheduledJobKeys.remove(id);
      if (jobKey == null) {
        throw new IllegalArgumentException("No scheduled job for function id " + id);
      }

      quartzScheduler.deleteJob(JobKey.jobKey(jobKey));

      LOG.info("Stopped {}", jobKey);
    }
  }

  /**
   * Available for testing, but anomalies need to be created with a valid anomaly function ID
   * (foreign key constraint).
   */
  public void runAdhocFile(String filePath, int existingFunctionId, String windowStartIsoString,
      String windowEndIsoString) throws Exception {
    synchronized (sync) {
      File file = new File(filePath);
      if (!file.exists() || file.isDirectory()) {
        throw new IllegalArgumentException("File does not exist or is a directory: " + file);
      }
      AnomalyFunctionSpec spec = reader.readValue(file, AnomalyFunctionSpec.class);
      spec.setId(existingFunctionId);
      runAdhocConfig(spec, windowStartIsoString, windowEndIsoString, filePath);
    }
  }

  public void runAdhocConfig(AnomalyFunctionSpec spec, String windowStartIsoString,
      String windowEndIsoString, String executionName) throws Exception, SchedulerException {
    AnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(spec);

    String triggerKey = String.format("file-based_anomaly_function_trigger_%s", executionName);
    Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey).startNow().build();

    String jobKey = String.format("file-based_anomaly_function_job_%s", executionName);
    buildAndScheduleJob(jobKey, trigger, anomalyFunction, spec, windowStartIsoString,
        windowEndIsoString);
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2 && args.length != 4) {
      System.err.println(
          "Arguments must be configYml functionSpecPath [startISO endISO, both hour aligned]");
      System.exit(1);
    }
    String thirdEyeConfigDir = args[0];
    System.setProperty("dw.rootDir", thirdEyeConfigDir);
    String detectorApplicationConfigFile = thirdEyeConfigDir + "/" + "detector.yml";
    String filePath = args[1];
    String startISO = null;
    String endISO = null;
    if (args.length == 4) {
      startISO = args[2];
      endISO = args[3];
    } else {
      DateTime now = DateTime.now().minusHours(3); // data delay.
      startISO = now.minusDays(7) // subtract 7 days to set up w/w comparison
          .minusHours(4) // subtract hours to specify what the length of the comparison window is
          .toString();
      endISO = now.toString();
    }
    int existingFunctionId = 1;
    new TestAnomalyApplication(filePath, startISO, endISO, TestType.FUNCTION, existingFunctionId)
        .run(new String[] {
            "server", detectorApplicationConfigFile
    });
  }

}
