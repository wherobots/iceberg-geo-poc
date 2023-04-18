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

import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.ParquetValueReaders;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKTReader;
import org.wololo.jts2geojson.GeoJSONReader;

public class ParquetGeometryValueReaders {

  private ParquetGeometryValueReaders() {}

  public static ParquetValueReader<Geometry> buildReader(
      Types.GeometryType geometryType, ColumnDescriptor desc) {
    switch (geometryType.encoding()) {
      case EWKB:
      case WKB:
        return new GeometryWKBReader(desc);
      case WKT:
        return new GeometryWKTReader(desc);
      case GEOJSON:
        return new GeometryGeoJSONReader(desc);
      default:
        throw new UnsupportedOperationException(
            "Unsupported geometry encoding: " + geometryType.encoding());
    }
  }

  private static class GeometryWKBReader extends ParquetValueReaders.PrimitiveReader<Geometry> {

    private final WKBReader wkbReader = new WKBReader();

    GeometryWKBReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public Geometry read(Geometry reuse) {
      Binary binary = column.nextBinary();
      try {
        return wkbReader.read(binary.getBytes());
      } catch (ParseException e) {
        throw new RuntimeException("Cannot parse byte array as geometry encoded in WKB", e);
      }
    }
  }

  private static class GeometryWKTReader extends ParquetValueReaders.PrimitiveReader<Geometry> {

    private final WKTReader wktReader = new WKTReader();

    GeometryWKTReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public Geometry read(Geometry reuse) {
      Binary binary = column.nextBinary();
      try {
        return wktReader.read(binary.toStringUsingUTF8());
      } catch (ParseException e) {
        throw new RuntimeException("Cannot parse byte array as geometry encoded in WKT", e);
      }
    }
  }

  private static class GeometryGeoJSONReader extends ParquetValueReaders.PrimitiveReader<Geometry> {

    private final GeoJSONReader geojsonReader = new GeoJSONReader();

    GeometryGeoJSONReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public Geometry read(Geometry reuse) {
      Binary binary = column.nextBinary();
      return geojsonReader.read(binary.toStringUsingUTF8());
    }
  }
}
