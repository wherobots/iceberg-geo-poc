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
package org.apache.iceberg.spark.functions;

import org.apache.iceberg.spark.geo.GeospatialLibraryAccessor;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableFunction;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Geometry;

public class HilbertFunction implements UnboundFunction {

  @Override
  public BoundFunction bind(StructType inputType) {
    StructField[] fields = inputType.fields();
    if (fields.length != 6) {
      throw new UnsupportedOperationException("Expected 6 arguments, got " + fields.length);
    }
    StructField geomField = fields[0];

    if (GeospatialLibraryAccessor.getGeometryType().sameType(geomField.dataType())
        || geomField.dataType().sameType(DataTypes.NullType)) {
      return new HilbertBoundFunction();
    }
    throw new UnsupportedOperationException("Expected first argument to be a geometry column");
  }

  @Override
  public String description() {
    return name()
        + "(geom, resolution, minX, minY, maxX, maxY) - Call Iceberg's hilbert transform\n"
        + "  geom :: geometry column to calculate hilbert index\n"
        + "  resolution :: hilbert curve resolution\n"
        + "  minX :: minimum x value of the world bound\n"
        + "  minY :: minimum y value of the world bound\n"
        + "  maxX :: maximum x value of the world bound\n"
        + "  maxY :: maximum y value of the world bound\n";
  }

  @Override
  public String name() {
    return "hilbert";
  }

  private static class HilbertBoundFunction implements ScalarFunction<Long> {

    private static Type icebergInputType = Types.GeometryType.get();

    @Override
    public DataType[] inputTypes() {
      return new DataType[] {
        GeospatialLibraryAccessor.getGeometryType(),
        DataTypes.IntegerType,
        DataTypes.DoubleType,
        DataTypes.DoubleType,
        DataTypes.DoubleType,
        DataTypes.DoubleType
      };
    }

    @Override
    public DataType resultType() {
      return DataTypes.LongType;
    }

    @Override
    public String name() {
      return "hilbert";
    }

    @Override
    public Long produceResult(InternalRow input) {
      if (input.isNullAt(0)) {
        return null;
      }
      Object obj = input.get(0, GeospatialLibraryAccessor.getGeometryType());
      Geometry geom = GeospatialLibraryAccessor.toJTS(obj);
      int resolution = input.getInt(1);
      double minX = input.getDouble(2);
      double minY = input.getDouble(3);
      double maxX = input.getDouble(4);
      double maxY = input.getDouble(5);
      Transform<Object, Long> hilbert = Transforms.hilbert(resolution, minX, minY, maxX, maxY);
      SerializableFunction<Object, Long> func = hilbert.bind(icebergInputType);
      return func.apply(geom);
    }
  }
}
