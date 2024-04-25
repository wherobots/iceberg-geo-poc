/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.source;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.source.metrics.EqualityDeleteFiles;
import org.apache.iceberg.spark.source.metrics.IndexedDeleteFiles;
import org.apache.iceberg.spark.source.metrics.NumDeletes;
import org.apache.iceberg.spark.source.metrics.NumSplits;
import org.apache.iceberg.spark.source.metrics.PositionalDeleteFiles;
import org.apache.iceberg.spark.source.metrics.ResultDataFiles;
import org.apache.iceberg.spark.source.metrics.ResultDeleteFiles;
import org.apache.iceberg.spark.source.metrics.ScannedDataManifests;
import org.apache.iceberg.spark.source.metrics.ScannedDeleteManifests;
import org.apache.iceberg.spark.source.metrics.SkippedDataFiles;
import org.apache.iceberg.spark.source.metrics.SkippedDataManifests;
import org.apache.iceberg.spark.source.metrics.SkippedDeleteFiles;
import org.apache.iceberg.spark.source.metrics.SkippedDeleteManifests;
import org.apache.iceberg.spark.source.metrics.TaskEqualityDeleteFiles;
import org.apache.iceberg.spark.source.metrics.TaskIndexedDeleteFiles;
import org.apache.iceberg.spark.source.metrics.TaskPositionalDeleteFiles;
import org.apache.iceberg.spark.source.metrics.TaskResultDataFiles;
import org.apache.iceberg.spark.source.metrics.TaskResultDeleteFiles;
import org.apache.iceberg.spark.source.metrics.TaskScannedDataManifests;
import org.apache.iceberg.spark.source.metrics.TaskScannedDeleteManifests;
import org.apache.iceberg.spark.source.metrics.TaskSkippedDataFiles;
import org.apache.iceberg.spark.source.metrics.TaskSkippedDataManifests;
import org.apache.iceberg.spark.source.metrics.TaskSkippedDeleteFiles;
import org.apache.iceberg.spark.source.metrics.TaskSkippedDeleteManifests;
import org.apache.iceberg.spark.source.metrics.TaskTotalDataFileSize;
import org.apache.iceberg.spark.source.metrics.TaskTotalDataManifests;
import org.apache.iceberg.spark.source.metrics.TaskTotalDeleteFileSize;
import org.apache.iceberg.spark.source.metrics.TaskTotalDeleteManifests;
import org.apache.iceberg.spark.source.metrics.TaskTotalPlanningDuration;
import org.apache.iceberg.spark.source.metrics.TotalDataFileSize;
import org.apache.iceberg.spark.source.metrics.TotalDataManifests;
import org.apache.iceberg.spark.source.metrics.TotalDeleteFileSize;
import org.apache.iceberg.spark.source.metrics.TotalDeleteManifests;
import org.apache.iceberg.spark.source.metrics.TotalPlanningDuration;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SparkScan implements Scan, SupportsReportStatistics {
  private static final Logger LOG = LoggerFactory.getLogger(SparkScan.class);

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final SparkReadConf readConf;
  private final boolean caseSensitive;
  private final Schema expectedSchema;
  private final List<Expression> filterExpressions;
  private final String branch;
  private final Supplier<ScanReport> scanReportSupplier;

  // lazy variables
  private StructType readSchema;

  SparkScan(
      JavaSparkContext sparkContext,
      Table table,
      SparkReadConf readConf,
      Schema expectedSchema,
      List<Expression> filters,
      Supplier<ScanReport> scanReportSupplier) {
    Schema snapshotSchema = SnapshotUtil.schemaFor(table, readConf.branch());
    SparkSchemaUtil.validateMetadataColumnReferences(snapshotSchema, expectedSchema);

    this.sparkContext = sparkContext;
    this.table = table;
    this.readConf = readConf;
    this.caseSensitive = readConf.caseSensitive();
    this.expectedSchema = expectedSchema;
    this.filterExpressions = filters != null ? filters : Collections.emptyList();
    this.branch = readConf.branch();
    this.scanReportSupplier = scanReportSupplier;
  }

  SparkScan(
      SparkSession spark,
      Table table,
      SparkReadConf readConf,
      Schema expectedSchema,
      List<Expression> filters,
      Supplier<ScanReport> scanReportSupplier) {
    this(
        JavaSparkContext.fromSparkContext(spark.sparkContext()),
        table,
        readConf,
        expectedSchema,
        filters,
        scanReportSupplier);
  }

  public SparkScan withExpressions(List<Expression> newFilters) {
    List<Expression> newFilterExpressions = Lists.newArrayList(this.filterExpressions());
    Schema schema = this.table().schema();
    boolean hasChanged = false;
    Set<String> filterExprSet =
        Sets.newHashSet(newFilterExpressions.stream().map(Object::toString).iterator());
    for (Expression expr : newFilters) {
      if (filterExprSet.contains(expr.toString())) {
        continue;
      }
      try {
        Binder.bind(schema.asStruct(), expr, caseSensitive);
        newFilterExpressions.add(expr);
        filterExprSet.add(expr.toString());
        hasChanged = true;
      } catch (ValidationException e) {
        LOG.info(
            "Failed to bind expression to table schema, skipping push down for this expression: {}. {}",
            expr,
            e.getMessage());
      }
    }
    if (hasChanged) {
      return withExpressionsInternal(newFilterExpressions);
    } else {
      return this;
    }
  }

  public abstract SparkScan withExpressionsInternal(List<Expression> newFilterExpressions);

  protected JavaSparkContext sparkContext() {
    return sparkContext;
  }

  protected Table table() {
    return table;
  }

  protected SparkReadConf readConf() {
    return readConf;
  }

  protected String branch() {
    return branch;
  }

  protected Supplier<ScanReport> scanReportSupplier() {
    return scanReportSupplier;
  }

  protected boolean caseSensitive() {
    return caseSensitive;
  }

  protected Schema expectedSchema() {
    return expectedSchema;
  }

  protected List<Expression> filterExpressions() {
    return filterExpressions;
  }

  protected Types.StructType groupingKeyType() {
    return Types.StructType.of();
  }

  protected abstract List<? extends ScanTaskGroup<?>> taskGroups();

  public long taskGroupSizeBytes() {
    long totalSizeBytes = 0;
    for (ScanTaskGroup<?> taskGroup : taskGroups()) {
      totalSizeBytes += taskGroup.sizeBytes();
    }
    return totalSizeBytes;
  }

  @Override
  public Batch toBatch() {
    return new SparkBatch(
        sparkContext, table, readConf, groupingKeyType(), taskGroups(), expectedSchema, hashCode());
  }

  @Override
  public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
    return new SparkMicroBatchStream(
        sparkContext, table, readConf, expectedSchema, checkpointLocation);
  }

  @Override
  public StructType readSchema() {
    if (readSchema == null) {
      this.readSchema = SparkSchemaUtil.convert(expectedSchema);
    }
    return readSchema;
  }

  @Override
  public Statistics estimateStatistics() {
    return estimateStatistics(SnapshotUtil.latestSnapshot(table, branch));
  }

  protected Statistics estimateStatistics(Snapshot snapshot) {
    // its a fresh table, no data
    if (snapshot == null) {
      return new Stats(0L, 0L);
    }

    // estimate stats using snapshot summary only for partitioned tables
    // (metadata tables are unpartitioned)
    if (!table.spec().isUnpartitioned() && filterExpressions.isEmpty()) {
      LOG.debug(
          "Using snapshot {} metadata to estimate statistics for table {}",
          snapshot.snapshotId(),
          table.name());
      long totalRecords = totalRecords(snapshot);
      return new Stats(SparkSchemaUtil.estimateSize(readSchema(), totalRecords), totalRecords);
    }

    long rowsCount = taskGroups().stream().mapToLong(ScanTaskGroup::estimatedRowsCount).sum();
    long sizeInBytes = SparkSchemaUtil.estimateSize(readSchema(), rowsCount);
    return new Stats(sizeInBytes, rowsCount);
  }

  private long totalRecords(Snapshot snapshot) {
    Map<String, String> summary = snapshot.summary();
    return PropertyUtil.propertyAsLong(summary, SnapshotSummary.TOTAL_RECORDS_PROP, Long.MAX_VALUE);
  }

  @Override
  public String description() {
    String groupingKeyFieldNamesAsString =
        groupingKeyType().fields().stream()
            .map(Types.NestedField::name)
            .collect(Collectors.joining(", "));

    return String.format(
        "%s (branch=%s) [filters=%s, groupedBy=%s]",
        table(), branch(), Spark3Util.describe(filterExpressions), groupingKeyFieldNamesAsString);
  }

  @Override
  public CustomTaskMetric[] reportDriverMetrics() {
    ScanReport scanReport = scanReportSupplier != null ? scanReportSupplier.get() : null;

    if (scanReport == null) {
      return new CustomTaskMetric[0];
    }

    List<CustomTaskMetric> driverMetrics = Lists.newArrayList();

    // common
    driverMetrics.add(TaskTotalPlanningDuration.from(scanReport));

    // data manifests
    driverMetrics.add(TaskTotalDataManifests.from(scanReport));
    driverMetrics.add(TaskScannedDataManifests.from(scanReport));
    driverMetrics.add(TaskSkippedDataManifests.from(scanReport));

    // data files
    driverMetrics.add(TaskResultDataFiles.from(scanReport));
    driverMetrics.add(TaskSkippedDataFiles.from(scanReport));
    driverMetrics.add(TaskTotalDataFileSize.from(scanReport));

    // delete manifests
    driverMetrics.add(TaskTotalDeleteManifests.from(scanReport));
    driverMetrics.add(TaskScannedDeleteManifests.from(scanReport));
    driverMetrics.add(TaskSkippedDeleteManifests.from(scanReport));

    // delete files
    driverMetrics.add(TaskTotalDeleteFileSize.from(scanReport));
    driverMetrics.add(TaskResultDeleteFiles.from(scanReport));
    driverMetrics.add(TaskEqualityDeleteFiles.from(scanReport));
    driverMetrics.add(TaskIndexedDeleteFiles.from(scanReport));
    driverMetrics.add(TaskPositionalDeleteFiles.from(scanReport));
    driverMetrics.add(TaskSkippedDeleteFiles.from(scanReport));

    return driverMetrics.toArray(new CustomTaskMetric[0]);
  }

  @Override
  public CustomMetric[] supportedCustomMetrics() {
    return new CustomMetric[] {
      // task metrics
      new NumSplits(),
      new NumDeletes(),

      // common
      new TotalPlanningDuration(),

      // data manifests
      new TotalDataManifests(),
      new ScannedDataManifests(),
      new SkippedDataManifests(),

      // data files
      new ResultDataFiles(),
      new SkippedDataFiles(),
      new TotalDataFileSize(),

      // delete manifests
      new TotalDeleteManifests(),
      new ScannedDeleteManifests(),
      new SkippedDeleteManifests(),

      // delete files
      new TotalDeleteFileSize(),
      new ResultDeleteFiles(),
      new EqualityDeleteFiles(),
      new IndexedDeleteFiles(),
      new PositionalDeleteFiles(),
      new SkippedDeleteFiles()
    };
  }

  protected long adjustSplitSize(List<? extends ScanTask> tasks, long splitSize) {
    if (readConf.splitSizeOption() == null && readConf.adaptiveSplitSizeEnabled()) {
      long scanSize = tasks.stream().mapToLong(ScanTask::sizeBytes).sum();
      int parallelism = readConf.parallelism();
      return TableScanUtil.adjustSplitSize(scanSize, parallelism, splitSize);
    } else {
      return splitSize;
    }
  }
}
