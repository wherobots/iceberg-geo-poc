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
package org.apache.iceberg.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.MetricsModes.MetricsMode;
import org.apache.iceberg.MetricsUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.BinaryUtil;
import org.apache.iceberg.util.UnicodeUtil;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.locationtech.jts.geom.Geometry;

public class ParquetUtil {
  // not meant to be instantiated
  private ParquetUtil() {}

  private static final long UNIX_EPOCH_JULIAN = 2_440_588L;

  public static Metrics fileMetrics(InputFile file, MetricsConfig metricsConfig) {
    return fileMetrics(file, metricsConfig, null);
  }

  public static Metrics fileMetrics(
      InputFile file, MetricsConfig metricsConfig, NameMapping nameMapping) {
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(file))) {
      return footerMetrics(reader.getFooter(), Stream.empty(), metricsConfig, nameMapping);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to read footer of file: %s", file);
    }
  }

  public static Metrics footerMetrics(
      Schema tableSchema,
      ParquetMetadata metadata,
      Stream<FieldMetrics<?>> fieldMetrics,
      MetricsConfig metricsConfig) {
    return footerMetrics(tableSchema, metadata, fieldMetrics, metricsConfig, null);
  }

  public static Metrics footerMetrics(
      ParquetMetadata metadata, Stream<FieldMetrics<?>> fieldMetrics, MetricsConfig metricsConfig) {
    return footerMetrics(metadata, fieldMetrics, metricsConfig, null);
  }

  public static Metrics footerMetrics(
      ParquetMetadata metadata,
      Stream<FieldMetrics<?>> fieldMetrics,
      MetricsConfig metricsConfig,
      NameMapping nameMapping) {
    return footerMetrics(null, metadata, fieldMetrics, metricsConfig, nameMapping);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public static Metrics footerMetrics(
      Schema tableSchema,
      ParquetMetadata metadata,
      Stream<FieldMetrics<?>> fieldMetrics,
      MetricsConfig metricsConfig,
      NameMapping nameMapping) {
    Preconditions.checkNotNull(fieldMetrics, "fieldMetrics should not be null");

    long rowCount = 0;
    Map<Integer, Long> columnSizes = Maps.newHashMap();
    Map<Integer, Long> valueCounts = Maps.newHashMap();
    Map<Integer, Long> nullValueCounts = Maps.newHashMap();
    Map<Integer, Literal<?>> lowerBounds = Maps.newHashMap();
    Map<Integer, Literal<?>> upperBounds = Maps.newHashMap();
    Map<Integer, Literal<?>> geomLowerBounds = Maps.newHashMap();
    Map<Integer, Literal<?>> geomUpperBounds = Maps.newHashMap();
    Set<Integer> missingStats = Sets.newHashSet();

    // ignore metrics for fields we failed to determine reliable IDs
    MessageType parquetTypeWithIds = getParquetTypeWithIds(metadata, nameMapping);
    // When tableSchema is not known, we'll derive fileSchema from parquet schema
    Schema fileSchema =
        tableSchema != null ? tableSchema : ParquetSchemaUtil.convert(parquetTypeWithIds);

    Map<Integer, FieldMetrics<?>> fieldMetricsMap =
        fieldMetrics.collect(Collectors.toMap(FieldMetrics::id, Function.identity()));

    List<BlockMetaData> blocks = metadata.getBlocks();
    for (BlockMetaData block : blocks) {
      rowCount += block.getRowCount();
      for (ColumnChunkMetaData column : block.getColumns()) {

        org.apache.parquet.schema.Type columnType =
            parquetTypeWithIds.getType(column.getPath().toArray());
        if (columnType == null || columnType.getId() == null) {
          // fileSchema may contain a subset of columns present in the file
          // as we prune columns we could not assign ids
          continue;
        }

        Integer fieldId = columnType.getId().intValue();
        increment(columnSizes, fieldId, column.getTotalSize());

        MetricsMode metricsMode = MetricsUtil.metricsMode(fileSchema, metricsConfig, fieldId);
        if (metricsMode == MetricsModes.None.get()) {
          continue;
        }
        increment(valueCounts, fieldId, column.getValueCount());

        Statistics stats = column.getStatistics();
        if (stats != null && !stats.isEmpty()) {
          increment(nullValueCounts, fieldId, stats.getNumNulls());

          Type icebergFieldType = fileSchema.findField(fieldId).type();

          // when there are metrics gathered by Iceberg for a column, we should use those instead
          // of the ones from Parquet.
          //
          // NOTE: There is one exception for geometry columns: to maintain backward compatibility,
          // we still
          // keep parquet stats for physical columns of geometry columns (usually min/max of binary
          // columns).
          // Statistics for geometry values collected by geometry value writers were stored in the
          // newly added
          // geom_lower_bound/geom_upper_bound properties in the manifest.
          if (metricsMode != MetricsModes.Counts.get()
              && (!fieldMetricsMap.containsKey(fieldId)
                  || icebergFieldType.typeId() == Type.TypeID.GEOMETRY)) {
            Types.NestedField field = fileSchema.findField(fieldId);
            if (field != null && stats.hasNonNullValue() && shouldStoreBounds(column, fileSchema)) {
              Type fieldType = field.type();
              // Interpret parquet values for geometry columns as the physical type of the geometry
              // column
              if (fieldType.typeId() == Type.TypeID.GEOMETRY) {
                fieldType = ((Types.GeometryType) fieldType).encoding().physicalType();
              }
              Literal<?> min =
                  ParquetConversions.fromParquetPrimitive(
                      fieldType, column.getPrimitiveType(), stats.genericGetMin());
              updateMin(lowerBounds, fieldId, fieldType, min, metricsMode);
              Literal<?> max =
                  ParquetConversions.fromParquetPrimitive(
                      fieldType, column.getPrimitiveType(), stats.genericGetMax());
              updateMax(upperBounds, fieldId, fieldType, max, metricsMode);
            }
          }
        } else {
          missingStats.add(fieldId);
        }
      }
    }

    // discard accumulated values if any stats were missing
    for (Integer fieldId : missingStats) {
      nullValueCounts.remove(fieldId);
      lowerBounds.remove(fieldId);
      upperBounds.remove(fieldId);
    }

    updateFromFieldMetrics(
        fieldMetricsMap,
        metricsConfig,
        fileSchema,
        lowerBounds,
        upperBounds,
        geomLowerBounds,
        geomUpperBounds);

    return new Metrics(
        rowCount,
        columnSizes,
        valueCounts,
        nullValueCounts,
        MetricsUtil.createNanValueCounts(
            fieldMetricsMap.values().stream(), metricsConfig, fileSchema),
        toBufferMap(fileSchema, lowerBounds, true),
        toBufferMap(fileSchema, upperBounds, true),
        toBufferMap(fileSchema, geomLowerBounds, false),
        toBufferMap(fileSchema, geomUpperBounds, false));
  }

  private static void updateFromFieldMetrics(
      Map<Integer, FieldMetrics<?>> idToFieldMetricsMap,
      MetricsConfig metricsConfig,
      Schema schema,
      Map<Integer, Literal<?>> lowerBounds,
      Map<Integer, Literal<?>> upperBounds,
      Map<Integer, Literal<?>> geomLowerBounds,
      Map<Integer, Literal<?>> geomUpperBounds) {
    idToFieldMetricsMap
        .entrySet()
        .forEach(
            entry -> {
              int fieldId = entry.getKey();
              FieldMetrics<?> metrics = entry.getValue();
              MetricsMode metricsMode = MetricsUtil.metricsMode(schema, metricsConfig, fieldId);

              // only check for MetricsModes.None, since we don't truncate float/double values.
              if (metricsMode != MetricsModes.None.get()) {
                if (!metrics.hasBounds()) {
                  lowerBounds.remove(fieldId);
                  upperBounds.remove(fieldId);
                } else if (metrics.upperBound() instanceof Float) {
                  lowerBounds.put(fieldId, Literal.of((Float) metrics.lowerBound()));
                  upperBounds.put(fieldId, Literal.of((Float) metrics.upperBound()));
                } else if (metrics.upperBound() instanceof Double) {
                  lowerBounds.put(fieldId, Literal.of((Double) metrics.lowerBound()));
                  upperBounds.put(fieldId, Literal.of((Double) metrics.upperBound()));
                } else if (metrics.upperBound() instanceof Geometry) {
                  geomLowerBounds.put(fieldId, Literal.of((Geometry) metrics.lowerBound()));
                  geomUpperBounds.put(fieldId, Literal.of((Geometry) metrics.upperBound()));
                } else {
                  throw new UnsupportedOperationException(
                      "Expected only float or double column metrics");
                }
              }
            });
  }

  private static MessageType getParquetTypeWithIds(
      ParquetMetadata metadata, NameMapping nameMapping) {
    MessageType type = metadata.getFileMetaData().getSchema();

    if (ParquetSchemaUtil.hasIds(type)) {
      return type;
    }

    if (nameMapping != null) {
      return ParquetSchemaUtil.applyNameMapping(type, nameMapping);
    }

    return ParquetSchemaUtil.addFallbackIds(type);
  }

  /**
   * Returns a list of offsets in ascending order determined by the starting position of the row
   * groups.
   */
  public static List<Long> getSplitOffsets(ParquetMetadata md) {
    List<Long> splitOffsets = Lists.newArrayListWithExpectedSize(md.getBlocks().size());
    for (BlockMetaData blockMetaData : md.getBlocks()) {
      splitOffsets.add(blockMetaData.getStartingPos());
    }
    Collections.sort(splitOffsets);
    return splitOffsets;
  }

  // we allow struct nesting, but not maps or arrays
  private static boolean shouldStoreBounds(ColumnChunkMetaData column, Schema schema) {
    if (column.getPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT96) {
      // stats for INT96 are not reliable
      return false;
    }

    ColumnPath columnPath = column.getPath();
    Iterator<String> pathIterator = columnPath.iterator();
    Type currentType = schema.asStruct();

    while (pathIterator.hasNext()) {
      if (currentType == null || !currentType.isStructType()) {
        return false;
      }
      String fieldName = pathIterator.next();
      currentType = currentType.asStructType().fieldType(fieldName);
    }

    return currentType != null && currentType.isPrimitiveType();
  }

  private static void increment(Map<Integer, Long> columns, int fieldId, long amount) {
    if (columns != null) {
      if (columns.containsKey(fieldId)) {
        columns.put(fieldId, columns.get(fieldId) + amount);
      } else {
        columns.put(fieldId, amount);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> void updateMin(
      Map<Integer, Literal<?>> lowerBounds,
      int id,
      Type type,
      Literal<T> min,
      MetricsMode metricsMode) {
    Literal<T> currentMin = (Literal<T>) lowerBounds.get(id);
    if (currentMin == null || min.comparator().compare(min.value(), currentMin.value()) < 0) {
      if (metricsMode == MetricsModes.Full.get()) {
        lowerBounds.put(id, min);
      } else {
        MetricsModes.Truncate truncateMode = (MetricsModes.Truncate) metricsMode;
        int truncateLength = truncateMode.length();
        switch (type.typeId()) {
          case STRING:
            lowerBounds.put(
                id, UnicodeUtil.truncateStringMin((Literal<CharSequence>) min, truncateLength));
            break;
          case FIXED:
          case BINARY:
            lowerBounds.put(
                id, BinaryUtil.truncateBinaryMin((Literal<ByteBuffer>) min, truncateLength));
            break;
          default:
            lowerBounds.put(id, min);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> void updateMax(
      Map<Integer, Literal<?>> upperBounds,
      int id,
      Type type,
      Literal<T> max,
      MetricsMode metricsMode) {
    Literal<T> currentMax = (Literal<T>) upperBounds.get(id);
    if (currentMax == null || max.comparator().compare(max.value(), currentMax.value()) > 0) {
      if (metricsMode == MetricsModes.Full.get()) {
        upperBounds.put(id, max);
      } else {
        MetricsModes.Truncate truncateMode = (MetricsModes.Truncate) metricsMode;
        int truncateLength = truncateMode.length();
        switch (type.typeId()) {
          case STRING:
            Literal<CharSequence> truncatedMaxString =
                UnicodeUtil.truncateStringMax((Literal<CharSequence>) max, truncateLength);
            if (truncatedMaxString != null) {
              upperBounds.put(id, truncatedMaxString);
            }
            break;
          case FIXED:
          case BINARY:
            Literal<ByteBuffer> truncatedMaxBinary =
                BinaryUtil.truncateBinaryMax((Literal<ByteBuffer>) max, truncateLength);
            if (truncatedMaxBinary != null) {
              upperBounds.put(id, truncatedMaxBinary);
            }
            break;
          default:
            upperBounds.put(id, max);
        }
      }
    }
  }

  private static Map<Integer, ByteBuffer> toBufferMap(
      Schema schema, Map<Integer, Literal<?>> map, boolean usePhysicalTypesForGeometry) {
    Map<Integer, ByteBuffer> bufferMap = Maps.newHashMap();
    for (Map.Entry<Integer, Literal<?>> entry : map.entrySet()) {
      Type fieldType = schema.findType(entry.getKey());
      if (fieldType.typeId() == Type.TypeID.GEOMETRY && usePhysicalTypesForGeometry) {
        fieldType = ((Types.GeometryType) fieldType).encoding().physicalType();
        bufferMap.put(
            entry.getKey(), Conversions.toByteBuffer(fieldType, entry.getValue().value()));
      } else {
        bufferMap.put(
            entry.getKey(), Conversions.toByteBuffer(fieldType, entry.getValue().value()));
      }
    }
    return bufferMap;
  }

  @SuppressWarnings("deprecation")
  public static boolean hasNonDictionaryPages(ColumnChunkMetaData meta) {
    EncodingStats stats = meta.getEncodingStats();
    if (stats != null) {
      return stats.hasNonDictionaryEncodedPages();
    }

    // without EncodingStats, fall back to testing the encoding list
    Set<Encoding> encodings = Sets.newHashSet(meta.getEncodings());
    if (encodings.remove(Encoding.PLAIN_DICTIONARY)) {
      // if remove returned true, PLAIN_DICTIONARY was present, which means at
      // least one page was dictionary encoded and 1.0 encodings are used

      // RLE and BIT_PACKED are only used for repetition or definition levels
      encodings.remove(Encoding.RLE);
      encodings.remove(Encoding.BIT_PACKED);

      // when empty, no encodings other than dictionary or rep/def levels
      return !encodings.isEmpty();
    } else {
      // if PLAIN_DICTIONARY wasn't present, then either the column is not
      // dictionary-encoded, or the 2.0 encoding, RLE_DICTIONARY, was used.
      // for 2.0, this cannot determine whether a page fell back without
      // page encoding stats
      return true;
    }
  }

  public static boolean hasNoBloomFilterPages(ColumnChunkMetaData meta) {
    return meta.getBloomFilterOffset() <= 0;
  }

  public static Dictionary readDictionary(ColumnDescriptor desc, PageReader pageSource) {
    DictionaryPage dictionaryPage = pageSource.readDictionaryPage();
    if (dictionaryPage != null) {
      try {
        return dictionaryPage.getEncoding().initDictionary(desc, dictionaryPage);
      } catch (IOException e) {
        throw new ParquetDecodingException("could not decode the dictionary for " + desc, e);
      }
    }
    return null;
  }

  public static boolean isIntType(PrimitiveType primitiveType) {
    if (primitiveType.getOriginalType() != null) {
      switch (primitiveType.getOriginalType()) {
        case INT_8:
        case INT_16:
        case INT_32:
        case DATE:
          return true;
        default:
          return false;
      }
    }
    return primitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32;
  }

  /**
   * Method to read timestamp (parquet Int96) from bytebuffer. Read 12 bytes in byteBuffer: 8 bytes
   * (time of day nanos) + 4 bytes(julianDay)
   */
  public static long extractTimestampInt96(ByteBuffer buffer) {
    // 8 bytes (time of day nanos)
    long timeOfDayNanos = buffer.getLong();
    // 4 bytes(julianDay)
    int julianDay = buffer.getInt();
    return TimeUnit.DAYS.toMicros(julianDay - UNIX_EPOCH_JULIAN)
        + TimeUnit.NANOSECONDS.toMicros(timeOfDayNanos);
  }
}
