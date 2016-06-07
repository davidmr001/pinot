package com.linkedin.thirdeye.client.comparison;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;

import com.google.common.collect.Range;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeResponse;
import com.linkedin.thirdeye.client.ThirdEyeResponseRow;
import com.linkedin.thirdeye.client.comparison.Row.Builder;

public class TimeOnTimeResponseParser {

  private ThirdEyeResponse baselineResponse;
  private ThirdEyeResponse currentResponse;
  private List<Range<DateTime>> baselineRanges;
  private List<Range<DateTime>> currentRanges;
  private TimeGranularity aggTimeGranularity;
  private List<String> groupByDimensions;

  private static String TIME_DIMENSION_JOINER_ESCAPED = "\\|";
  private static String TIME_DIMENSION_JOINER = "|";

  public TimeOnTimeResponseParser(ThirdEyeResponse baselineResponse,
      ThirdEyeResponse currentResponse, List<Range<DateTime>> baselineRanges,
      List<Range<DateTime>> currentRanges, TimeGranularity timeGranularity,
      List<String> groupByDimensions) {
    this.baselineResponse = baselineResponse;
    this.currentResponse = currentResponse;
    this.baselineRanges = baselineRanges;
    this.currentRanges = currentRanges;
    this.aggTimeGranularity = timeGranularity;
    this.groupByDimensions = groupByDimensions;
  }

  List<Row> parseResponse() {
    if (baselineResponse == null || currentResponse == null) {
      return Collections.emptyList();
    }
    boolean hasGroupByDimensions = false;
    if (groupByDimensions != null && groupByDimensions.size() > 0) {
      hasGroupByDimensions = true;
    }
    boolean hasGroupByTime = false;
    if (aggTimeGranularity != null) {
      hasGroupByTime = true;
    }

    Map<String, ThirdEyeResponseRow> baselineResponseMap;
    Map<String, ThirdEyeResponseRow> currentResponseMap;

    List<Row> rows = new ArrayList<>();
    if (hasGroupByTime) {

      int numTimeBuckets = baselineRanges.size();

      if (hasGroupByDimensions) {
        // group by time only //tabular
        baselineResponseMap = createContributorResponseMap(baselineResponse);
        currentResponseMap = createContributorResponseMap(currentResponse);

        Set<String> timeDimensionValues = new HashSet<>();
        timeDimensionValues.addAll(baselineResponseMap.keySet());
        timeDimensionValues.addAll(currentResponseMap.keySet());
        Set<String> dimensionValues = new HashSet<>();
        for (String timeDimensionValue : timeDimensionValues) {
          dimensionValues.add(timeDimensionValue.split(TIME_DIMENSION_JOINER_ESCAPED)[1]);
        }

        String dimensionName = baselineResponse.getGroupKeyColumns().get(1);

        for (String dimension : dimensionValues) {
          for (int timeBucketId = 0; timeBucketId < numTimeBuckets; timeBucketId++) {
            Range<DateTime> baselineTimeRange = baselineRanges.get(timeBucketId);
            Range<DateTime> currentTimeRange = currentRanges.get(timeBucketId);

            //compute the time|dimension key
            String baselineTimeDimensionValue = timeBucketId + TIME_DIMENSION_JOINER + dimension;
            String currentTimeDimensionValue = timeBucketId + TIME_DIMENSION_JOINER + dimension;

            ThirdEyeResponseRow baselineRow = baselineResponseMap.get(baselineTimeDimensionValue);
            ThirdEyeResponseRow currentRow = currentResponseMap.get(currentTimeDimensionValue);

            Row.Builder builder = new Row.Builder();
            builder.setBaselineStart(baselineTimeRange.lowerEndpoint());
            builder.setBaselineEnd(baselineTimeRange.upperEndpoint());
            builder.setCurrentStart(currentTimeRange.lowerEndpoint());
            builder.setCurrentEnd(currentTimeRange.upperEndpoint());

            builder.setDimensionName(dimensionName);
            builder.setDimensionValue(dimension);

            addMetric(baselineRow, currentRow, builder);

            Row row = builder.build();
            rows.add(row);
          }
        }

      } else {
        // group by time only //tabular
        baselineResponseMap = createTabularResponseMap(baselineResponse);
        currentResponseMap = createTabularResponseMap(currentResponse);

        for (int timeBucketId = 0; timeBucketId < numTimeBuckets; timeBucketId++) {
          Range<DateTime> baselineTimeRange = baselineRanges.get(timeBucketId);
          ThirdEyeResponseRow baselineRow =
              baselineResponseMap.get(String.valueOf(timeBucketId));

          Range<DateTime> currentTimeRange = currentRanges.get(timeBucketId);
          ThirdEyeResponseRow currentRow = currentResponseMap.get(String.valueOf(timeBucketId));

          Row.Builder builder = new Row.Builder();
          builder.setBaselineStart(baselineTimeRange.lowerEndpoint());
          builder.setBaselineEnd(baselineTimeRange.upperEndpoint());
          builder.setCurrentStart(currentTimeRange.lowerEndpoint());
          builder.setCurrentEnd(currentTimeRange.upperEndpoint());

          addMetric(baselineRow, currentRow, builder);
          Row row = builder.build();
          rows.add(row);
        }
      }
    } else {
      // no break down by time
      if (hasGroupByDimensions) {
        // group by dimensions only //heatmap
        baselineResponseMap = createHeatmapResponseMap(baselineResponse);
        currentResponseMap = createHeatmapResponseMap(currentResponse);

        String dimensionName = baselineResponse.getGroupKeyColumns().get(0);

        Set<String> dimensionValues = new HashSet<>();
        dimensionValues.addAll(baselineResponseMap.keySet());
        dimensionValues.addAll(currentResponseMap.keySet());

        for (String dimensionValue : dimensionValues) {
          Row.Builder builder = new Row.Builder();
          builder.setBaselineStart(baselineRanges.get(0).lowerEndpoint());
          builder.setBaselineEnd(baselineRanges.get(0).upperEndpoint());
          builder.setCurrentStart(currentRanges.get(0).lowerEndpoint());
          builder.setCurrentEnd(currentRanges.get(0).upperEndpoint());

          builder.setDimensionName(dimensionName);
          builder.setDimensionValue(dimensionValue);

          ThirdEyeResponseRow baselineRow = baselineResponseMap.get(dimensionValue);
          ThirdEyeResponseRow currentRow = currentResponseMap.get(dimensionValue);

          addMetric(baselineRow, currentRow, builder);

          Row row = builder.build();
          rows.add(row);
        }

      } else {
        // no group by
      }
    }

    return rows;
  }

  private static Map<String, ThirdEyeResponseRow> createContributorResponseMap(
      ThirdEyeResponse thirdEyeResponse) {
    Map<String, ThirdEyeResponseRow> responseMap;
    responseMap = new HashMap<>();
    int numRows = thirdEyeResponse.getNumRows();
    for (int i = 0; i < numRows; i++) {
      ThirdEyeResponseRow thirdEyeResponseRow = thirdEyeResponse.getRow(i);
      responseMap.put(
          thirdEyeResponseRow.getTimeBucketId() + "|" + thirdEyeResponseRow.getDimensions().get(0),
          thirdEyeResponseRow);
    }
    return responseMap;
  }

  private static Map<String, ThirdEyeResponseRow> createTabularResponseMap(
      ThirdEyeResponse thirdEyeResponse) {
    Map<String, ThirdEyeResponseRow> responseMap;
    responseMap = new HashMap<>();
    int numRows = thirdEyeResponse.getNumRows();
    for (int i = 0; i < numRows; i++) {
      ThirdEyeResponseRow thirdEyeResponseRow = thirdEyeResponse.getRow(i);
      responseMap.put(String.valueOf(thirdEyeResponseRow.getTimeBucketId()), thirdEyeResponseRow);
    }
    return responseMap;
  }

  private static Map<String, ThirdEyeResponseRow> createHeatmapResponseMap(
      ThirdEyeResponse thirdEyeResponse) {
    Map<String, ThirdEyeResponseRow> responseMap;
    responseMap = new HashMap<>();
    int numRows = thirdEyeResponse.getNumRows();
    for (int i = 0; i < numRows; i++) {
      ThirdEyeResponseRow thirdEyeResponseRow = thirdEyeResponse.getRow(i);
      responseMap.put(thirdEyeResponseRow.getDimensions().get(0), thirdEyeResponseRow);
    }
    return responseMap;
  }

  private void addMetric(ThirdEyeResponseRow baselineRow, ThirdEyeResponseRow currentRow, Builder builder) {

    List<MetricFunction> metricFunctions = baselineResponse.getMetricFunctions();

    for (int i = 0; i < metricFunctions.size(); i++) {
      MetricFunction metricFunction = metricFunctions.get(i);
      double baselineValue = 0;
      if (baselineRow != null) {
        baselineValue = baselineRow.getMetrics().get(i);
      }
      double currentValue = 0;
      if (currentRow != null) {
        currentValue = currentRow.getMetrics().get(i);
      }
      builder.addMetric(metricFunction.getMetricName(), baselineValue, currentValue);
    }
  }

}
