package com.linkedin.thirdeye.client.comparison;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.joda.time.DateTime;

import com.google.common.collect.Range;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.client.TimeRangeUtils;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.comparison.Row.Metric;
import com.linkedin.thirdeye.client.ThirdEyeResponse;
import com.linkedin.thirdeye.dashboard.Utils;

public class TimeOnTimeComparisonHandler {
  private final QueryCache queryCache;

  public TimeOnTimeComparisonHandler(QueryCache queryCache) {
    this.queryCache = queryCache;
  }

  public TimeOnTimeComparisonResponse handle(TimeOnTimeComparisonRequest comparisonRequest)
      throws Exception {
    ThirdEyeRequestBuilder builder = new ThirdEyeRequestBuilder();
    builder.setCollection(comparisonRequest.getCollectionName());
    List<Range<DateTime>> baselineTimeranges = new ArrayList<>();
    List<Range<DateTime>> currentTimeranges = new ArrayList<>();
    TimeGranularity aggregationTimeGranularity = comparisonRequest.getAggregationTimeGranularity();
    List<Range<DateTime>> timeRanges;
    // baseline time ranges
    timeRanges = TimeRangeUtils.computeTimeRanges(aggregationTimeGranularity,
        comparisonRequest.getBaselineStart(), comparisonRequest.getBaselineEnd());
    baselineTimeranges.addAll(timeRanges);
    // current time ranges
    timeRanges = TimeRangeUtils.computeTimeRanges(aggregationTimeGranularity,
        comparisonRequest.getCurrentStart(), comparisonRequest.getCurrentEnd());
    currentTimeranges.addAll(timeRanges);

    int numTimeRanges = baselineTimeranges.size();
    boolean hasGroupByDimensions =
        CollectionUtils.isNotEmpty(comparisonRequest.getGroupByDimensions());
    List<Map<ThirdEyeRequest, Future<ThirdEyeResponse>>> responseFutureList = new ArrayList<>();
    List<TimeOnTimeComparisonRequest> comparisonRequests = new ArrayList<>(numTimeRanges);

    for (int i = 0; i < numTimeRanges; i++) {
      Range<DateTime> baselineRange = baselineTimeranges.get(i);
      Range<DateTime> currentRange = currentTimeranges.get(i);
      // generate multiple request objects
      TimeOnTimeComparisonRequest request = new TimeOnTimeComparisonRequest(comparisonRequest);
      request.setBaselineStart(baselineRange.lowerEndpoint());
      request.setBaselineEnd(baselineRange.upperEndpoint());
      request.setCurrentStart(currentRange.lowerEndpoint());
      request.setCurrentEnd(currentRange.upperEndpoint());
      comparisonRequests.add(request);
      if (hasGroupByDimensions) {
        List<ThirdEyeRequest> requests =
            ThirdEyeRequestGenerator.generateRequestsForGroupByDimensions(request);
        Map<ThirdEyeRequest, Future<ThirdEyeResponse>> queryResultMap =
            queryCache.getQueryResultsAsync(requests);
        responseFutureList.add(queryResultMap);
      } else {
        List<ThirdEyeRequest> requests =
            ThirdEyeRequestGenerator.generateRequestsForAggregation(request);
        Map<ThirdEyeRequest, Future<ThirdEyeResponse>> queryResultMap =
            queryCache.getQueryResultsAsync(requests);
        responseFutureList.add(queryResultMap);
      }
    }
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < timeRanges.size(); i++) {
      Map<ThirdEyeRequest, Future<ThirdEyeResponse>> futureResponseMap = responseFutureList.get(i);
      Map<ThirdEyeRequest, ThirdEyeResponse> responseMap = new LinkedHashMap<>();
      for (Entry<ThirdEyeRequest, Future<ThirdEyeResponse>> entry : futureResponseMap.entrySet()) {
        responseMap.put(entry.getKey(), entry.getValue().get(60, TimeUnit.SECONDS));
      }
      if (hasGroupByDimensions) {
        List<Row> rowList = TimeOnTimeResponseParser
            .parseGroupByDimensionResponse(comparisonRequests.get(i), responseMap);
        rows.addAll(rowList);
      } else {
        Row row = TimeOnTimeResponseParser.parseAggregationOnlyResponse(comparisonRequests.get(i),
            responseMap);
        rows.add(row);
      }
    }

    // compute list of derived expressions

    List<MetricFunction> metricFunctionsFromExpressions =
        Utils.computeMetricFunctionsFromExpressions(comparisonRequest.getMetricExpressions());
    Set<String> metricNameSet = new HashSet<>();
    for (MetricFunction function : metricFunctionsFromExpressions) {
      metricNameSet.add(function.getMetricName());
    }
    List<MetricExpression> derivedMetricExpressions = new ArrayList<>();
    for (MetricExpression expression : comparisonRequest.getMetricExpressions()) {
      if (!metricNameSet.contains(expression.getExpressionName())) {
        derivedMetricExpressions.add(expression);
      }
    }

    // add metric expressions
    if (derivedMetricExpressions.size() > 0) {
      Map<String, Double> baselineValueContext = new HashMap<>();
      Map<String, Double> currentValueContext = new HashMap<>();
      for (Row row : rows) {
        baselineValueContext.clear();
        currentValueContext.clear();
        List<Metric> metrics = row.getMetrics();
        // baseline value
        for (Metric metric : metrics) {
          baselineValueContext.put(metric.getMetricName(), metric.getBaselineValue());
          currentValueContext.put(metric.getMetricName(), metric.getCurrentValue());
        }
        for (MetricExpression expression : derivedMetricExpressions) {
          String derivedMetricExpression = expression.getExpression();
          double derivedMetricBaselineValue =
              MetricExpression.evaluateExpression(derivedMetricExpression, baselineValueContext);
          if (Double.isInfinite(derivedMetricBaselineValue) || Double.isNaN(derivedMetricBaselineValue)) {
            derivedMetricBaselineValue = 0;
          }
          double currentMetricBaselineValue =
              MetricExpression.evaluateExpression(derivedMetricExpression, currentValueContext);
          if (Double.isInfinite(currentMetricBaselineValue) || Double.isNaN(currentMetricBaselineValue)) {
            currentMetricBaselineValue = 0;
          }

          row.getMetrics().add(new Metric(expression.getExpressionName(),
              derivedMetricBaselineValue, currentMetricBaselineValue));
        }
      }
    }

    return new TimeOnTimeComparisonResponse(rows);
  }

  /**
   * pure aggregation no group by
   * Generates the following queries
   * <code>
   * select sum(m1), sum(m2) from T where filters AND (t1 between baselineStart and baselineEnd)
   * select sum(m1), sum(m2) from T where filters AND (t1 between currentStart and currentEnd)
   * @param comparisonRequest
   * @throws Exception
   */
  public Row handleAggregateOnly(TimeOnTimeComparisonRequest comparisonRequest) throws Exception {

    List<ThirdEyeRequest> requests =
        ThirdEyeRequestGenerator.generateRequestsForAggregation(comparisonRequest);
    Map<ThirdEyeRequest, ThirdEyeResponse> queryResultMap =
        queryCache.getQueryResultsAsyncAndWait(requests);
    Row row =
        TimeOnTimeResponseParser.parseAggregationOnlyResponse(comparisonRequest, queryResultMap);
    return row;
  }

  /**
   * Generates the following queries
   * <code>
   * select sum(m1), sum(m2) from T where filters AND (t1 between baselineStart and baselineEnd)
   * select sum(m1), sum(m2) from T where filters AND (t1 between currentStart and currentEnd)
   * FOR EACH DIMENSION in group by
   * select sum(m1), sum(m2) from T where filters AND (t1 between baselineStart and baselineEnd) group by dimension(i) top 25
   * select sum(m1), sum(m2) from T where filters AND (t1 between currentStart and currentEnd) group by dimension(i) top 25
   *
   * For each dimension we will add additional group called OTHER which will cater to the remaining
   * values for each dimension
   * </code>
   * @param comparisonRequest
   * @throws Exception
   */
  public List<Row> handleGroupByDimension(TimeOnTimeComparisonRequest comparisonRequest)
      throws Exception {
    List<ThirdEyeRequest> requests =
        ThirdEyeRequestGenerator.generateRequestsForGroupByDimensions(comparisonRequest);

    Map<ThirdEyeRequest, ThirdEyeResponse> queryResultMap =
        queryCache.getQueryResultsAsyncAndWait(requests);

    return TimeOnTimeResponseParser.parseGroupByDimensionResponse(comparisonRequest,
        queryResultMap);

  }

  public QueryCache getQueryCache() {
    return queryCache;
  }

  public ThirdEyeClient getClient() {
    return queryCache.getClient();
  }

}
