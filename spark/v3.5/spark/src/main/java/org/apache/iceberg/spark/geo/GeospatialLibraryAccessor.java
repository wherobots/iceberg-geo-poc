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
package org.apache.iceberg.spark.geo;

import java.util.ServiceLoader;
import org.apache.iceberg.spark.geo.spi.GeospatialLibrary;
import org.apache.iceberg.spark.geo.spi.GeospatialLibraryProvider;
import org.apache.spark.sql.types.DataType;
import org.locationtech.jts.geom.Geometry;

public class GeospatialLibraryAccessor {
  private GeospatialLibraryAccessor() {}

  private static final GeospatialLibrary instance = load();

  public static GeospatialLibrary getGeospatialLibrary() {
    return instance;
  }

  public static boolean isGeospatialLibraryAvailable() {
    return instance != null;
  }

  public static DataType getGeometryType() {
    checkGeospatialLibrary();
    return instance.getGeometryType();
  }

  public static Object fromJTS(Geometry jtsGeometry) {
    checkGeospatialLibrary();
    return instance.fromJTS(jtsGeometry);
  }

  public static Geometry toJTS(Object geometry) {
    checkGeospatialLibrary();
    return instance.toJTS(geometry);
  }

  public static boolean isSpatialFilter(
      org.apache.spark.sql.catalyst.expressions.Expression sparkExpression) {
    checkGeospatialLibrary();
    return instance.isSpatialFilter(sparkExpression);
  }

  public static org.apache.iceberg.expressions.Expression translateToIceberg(
      org.apache.spark.sql.catalyst.expressions.Expression sparkExpression) {
    checkGeospatialLibrary();
    return instance.translateToIceberg(sparkExpression);
  }

  private static void checkGeospatialLibrary() {
    if (instance == null) {
      throw new UnsupportedOperationException("No geospatial library found");
    }
  }

  public static GeospatialLibrary load() {
    ServiceLoader<GeospatialLibraryProvider> provides =
        ServiceLoader.load(GeospatialLibraryProvider.class);
    for (GeospatialLibraryProvider provider : provides) {
      return provider.create();
    }

    // No geospatial library found
    return null;
  }
}
