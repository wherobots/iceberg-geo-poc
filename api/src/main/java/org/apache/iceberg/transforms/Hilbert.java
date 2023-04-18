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
package org.apache.iceberg.transforms;

import java.io.Serializable;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.util.SerializableFunction;
import org.apache.iceberg.util.havasu.HilbertCurve2D;
import org.locationtech.jts.geom.Geometry;

class Hilbert<T> implements Transform<T, Long>, Serializable {

  static <T> Hilbert<T> get(int resolution, double minX, double minY, double maxX, double maxY) {
    return new Hilbert<>(resolution, minX, minY, maxX, maxY);
  }

  static <T> Hilbert<T> get(int resolution) {
    return get(resolution, -180, -90, 180, 90);
  }

  static <T> Hilbert<T> get(HilbertCurve2D curve) {
    return new Hilbert<>(curve);
  }

  private final int resolution;
  private final double minX;
  private final double minY;
  private final double maxX;
  private final double maxY;
  private transient HilbertCurve2D lazyCurve;

  private Hilbert(int resolution, double minX, double minY, double maxX, double maxY) {
    Preconditions.checkArgument(resolution > 0, "Resolution must be positive: %s", resolution);
    Preconditions.checkArgument(minX < maxX, "Invalid bounds: %s >= %s", minX, maxX);
    Preconditions.checkArgument(minY < maxY, "Invalid bounds: %s >= %s", minY, maxY);
    this.resolution = resolution;
    this.minX = minX;
    this.minY = minY;
    this.maxX = maxX;
    this.maxY = maxY;
  }

  private Hilbert(HilbertCurve2D curve) {
    this.resolution = curve.resolution();
    this.minX = curve.minX();
    this.minY = curve.minY();
    this.maxX = curve.maxX();
    this.maxY = curve.maxY();
    this.lazyCurve = curve;
  }

  @Override
  public boolean canTransform(Type type) {
    return type.typeId() == Type.TypeID.GEOMETRY;
  }

  @Override
  public Type getResultType(Type sourceType) {
    return LongType.get();
  }

  @Override
  public SerializableFunction<T, Long> bind(Type type) {
    Preconditions.checkArgument(canTransform(type), "Cannot bind to unsupported type: %s", type);
    return this::apply;
  }

  @Override
  public Long apply(T value) {
    if (value == null) {
      return null;
    }
    Geometry geom = (Geometry) value;
    return curve().index(geom);
  }

  @Override
  public UnboundPredicate<Long> project(String name, BoundPredicate<T> predicate) {
    // Hilbert index does not support partition predicate transformation, we'll simply rely on
    // the geometry column metrics to prune data.
    return null;
  }

  @Override
  public UnboundPredicate<Long> projectStrict(String name, BoundPredicate<T> predicate) {
    // Hilbert index does not support partition predicate transformation, we'll simply rely on
    // the geometry column metrics to prune data.
    return null;
  }

  public HilbertCurve2D curve() {
    if (lazyCurve == null) {
      lazyCurve = new HilbertCurve2D(resolution, minX, minY, maxX, maxY);
    }
    return lazyCurve;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof Hilbert)) {
      return false;
    }

    Hilbert<?> hilbertIndex = (Hilbert<?>) o;
    return curve().equals(hilbertIndex.curve());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(curve());
  }

  @Override
  public String toString() {
    return String.format(
        "hilbert[%d,%f,%f,%f,%f]",
        curve().resolution(), curve().minX(), curve().minY(), curve().maxX(), curve().maxY());
  }
}
