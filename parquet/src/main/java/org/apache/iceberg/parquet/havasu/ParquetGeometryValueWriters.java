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
package org.apache.iceberg.parquet.havasu;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.havasu.GeometryFieldMetrics;
import org.apache.iceberg.parquet.ParquetValueWriters;
import org.apache.iceberg.types.Types.GeometryType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTWriter;
import org.wololo.jts2geojson.GeoJSONWriter;

public class ParquetGeometryValueWriters {

  private ParquetGeometryValueWriters() {}

  public static ParquetValueWriters.PrimitiveWriter<Geometry> buildWriter(
      GeometryType geometryType, ColumnDescriptor desc) {
    switch (geometryType.encoding()) {
      case EWKB:
        return new GeometryEWKBWriter(desc);
      case WKB:
        return new GeometryWKBWriter(desc);
      case WKT:
        return new GeometryWKTWriter(desc);
      case GEOJSON:
        return new GeometryGeoJSONWriter(desc);
      default:
        throw new UnsupportedOperationException(
            "Unsupported geometry encoding: " + geometryType.encoding());
    }
  }

  private abstract static class GeometryWKBFamilyWriter
      extends ParquetValueWriters.PrimitiveWriter<Geometry> {

    private final GeometryFieldMetrics.GenericBuilder metricsBuilder;
    private final boolean includeSRID;

    GeometryWKBFamilyWriter(ColumnDescriptor desc, boolean includeSRID) {
      super(desc);
      int id = desc.getPrimitiveType().getId().intValue();
      this.includeSRID = includeSRID;
      metricsBuilder = new GeometryFieldMetrics.GenericBuilder(id);
    }

    @Override
    public void write(int rl, Geometry geom) {
      WKBWriter wkbWriter = new WKBWriter(getDimension(geom), includeSRID);
      byte[] ewkb = wkbWriter.write(geom);
      column.writeBinary(rl, Binary.fromReusedByteArray(ewkb));
      metricsBuilder.add(geom);
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return Stream.of(metricsBuilder.build());
    }
  }

  private static class GeometryEWKBWriter extends GeometryWKBFamilyWriter {

    GeometryEWKBWriter(ColumnDescriptor desc) {
      super(desc, true);
    }
  }

  private static class GeometryWKBWriter extends GeometryWKBFamilyWriter {

    GeometryWKBWriter(ColumnDescriptor desc) {
      super(desc, false);
    }
  }

  private static class GeometryWKTWriter extends ParquetValueWriters.PrimitiveWriter<Geometry> {

    private final GeometryFieldMetrics.GenericBuilder metricsBuilder;

    GeometryWKTWriter(ColumnDescriptor desc) {
      super(desc);
      int id = desc.getPrimitiveType().getId().intValue();
      metricsBuilder = new GeometryFieldMetrics.GenericBuilder(id);
    }

    @Override
    public void write(int rl, Geometry geom) {
      WKTWriter wktWriter = new WKTWriter(getDimension(geom));
      String wkt = wktWriter.write(geom);
      column.writeBinary(rl, Binary.fromReusedByteArray(wkt.getBytes(StandardCharsets.UTF_8)));
      metricsBuilder.add(geom);
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return Stream.of(metricsBuilder.build());
    }
  }

  private static class GeometryGeoJSONWriter extends ParquetValueWriters.PrimitiveWriter<Geometry> {

    private final GeometryFieldMetrics.GenericBuilder metricsBuilder;

    GeometryGeoJSONWriter(ColumnDescriptor desc) {
      super(desc);
      int id = desc.getPrimitiveType().getId().intValue();
      metricsBuilder = new GeometryFieldMetrics.GenericBuilder(id);
    }

    @Override
    public void write(int rl, Geometry geom) {
      GeoJSONWriter geojsonWriter = new GeoJSONWriter();
      String geojson = geojsonWriter.write(geom).toString();
      column.writeBinary(rl, Binary.fromReusedByteArray(geojson.getBytes(StandardCharsets.UTF_8)));
      metricsBuilder.add(geom);
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return Stream.of(metricsBuilder.build());
    }
  }

  private static int getDimension(Geometry geom) {
    return geom.getCoordinate() != null && !java.lang.Double.isNaN(geom.getCoordinate().getZ())
        ? 3
        : 2;
  }
}
