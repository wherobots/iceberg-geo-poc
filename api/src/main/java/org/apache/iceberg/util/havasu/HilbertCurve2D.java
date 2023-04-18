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
package org.apache.iceberg.util.havasu;

import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.uzaygezen.core.BitVector;
import org.apache.iceberg.relocated.com.google.uzaygezen.core.BitVectorFactories;
import org.apache.iceberg.relocated.com.google.uzaygezen.core.CompactHilbertCurve;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/** Class for calculating the index of a geometry using Hilbert space filling curve. */
public class HilbertCurve2D {

  private final int resolution;
  private final double minX;
  private final double minY;
  private final double maxX;
  private final double maxY;
  private final double scaleX;
  private final double scaleY;
  private final CompactHilbertCurve chc;

  /**
   * Construct a 2D Hilbert curve index calculator
   *
   * @param resolution resolution of the Hilbert curve
   * @param minX minimum x value of the indexed plane
   * @param minY minimum y value of the indexed plane
   * @param maxX maximum x value of the indexed plane
   * @param maxY maximum y value of the indexed plane
   */
  public HilbertCurve2D(int resolution, double minX, double minY, double maxX, double maxY) {
    Preconditions.checkArgument(resolution > 0, "Resolution must be positive: %s", resolution);
    Preconditions.checkArgument(minX < maxX, "Invalid bounds: %s >= %s", minX, maxX);
    Preconditions.checkArgument(minY < maxY, "Invalid bounds: %s >= %s", minY, maxY);
    this.resolution = resolution;
    this.minX = minX;
    this.minY = minY;
    this.maxX = maxX;
    this.maxY = maxY;
    this.chc = new CompactHilbertCurve(new int[] {resolution, resolution});

    long precision = (long) Math.pow(2, resolution);
    this.scaleX = (precision - 1) / (maxX - minX);
    this.scaleY = (precision - 1) / (maxY - minY);
  }

  /**
   * Calculate the hilbert index of the geometry. The index is calculated using the center of the
   * envelope of the geometry.
   *
   * @param geom geometry
   * @return hilbert index of the geometry
   */
  public Long index(Geometry geom) {
    if (geom == null) {
      return null;
    }

    // Get center of the envelope
    Envelope envelope = geom.getEnvelopeInternal();
    if (envelope.isNull()) {
      return null;
    }
    Coordinate center = envelope.centre();

    // Get the index of the center
    return index(center.x, center.y);
  }

  /**
   * Calculate the hilbert index of the point.
   *
   * @param x x coordinate
   * @param y y coordinate
   * @return hilbert index of the point
   */
  @SuppressWarnings("checkstyle:ParameterName")
  public Long index(double x, double y) {
    // Check if the point is within the bounds
    if (x > maxX || y > maxY || x < minX || y < minY) {
      return null;
    }

    // Get normalized coordinates of the center
    long xNorm = (long) ((x - minX) * scaleX);
    long yNorm = (long) ((y - minY) * scaleY);

    // Get the index of the normalized coordinates
    final BitVector[] p = {
      BitVectorFactories.OPTIMAL.apply(resolution), BitVectorFactories.OPTIMAL.apply(resolution)
    };
    p[0].copyFrom(xNorm);
    p[1].copyFrom(yNorm);
    BitVector index = BitVectorFactories.OPTIMAL.apply(resolution * 2);
    chc.index(p, 0, index);
    return index.toLong();
  }

  public int resolution() {
    return resolution;
  }

  public double minX() {
    return minX;
  }

  public double minY() {
    return minY;
  }

  public double maxX() {
    return maxX;
  }

  public double maxY() {
    return maxY;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(resolution, minX, minY, maxX, maxY);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof HilbertCurve2D)) {
      return false;
    }
    HilbertCurve2D other = (HilbertCurve2D) o;
    return resolution == other.resolution
        && minX == other.minX
        && minY == other.minY
        && maxX == other.maxX
        && maxY == other.maxY;
  }

  @Override
  public String toString() {
    return "HilbertCurve2D{"
        + "resolution="
        + resolution
        + ", minX="
        + minX
        + ", minY="
        + minY
        + ", maxX="
        + maxX
        + ", maxY="
        + maxY
        + '}';
  }
}
