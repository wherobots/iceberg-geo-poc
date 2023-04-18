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

import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableFunction;
import org.apache.iceberg.util.havasu.HilbertCurve2D;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class TestHilbert {
  private static final GeometryFactory factory = new GeometryFactory();

  @Test
  public void testHilbertStringify() {
    Transform<?, ?> transform = Transforms.fromString("hilbert[10, -1e10, -2e10, 10.1, 20.2]");
    Assert.assertTrue(transform instanceof Hilbert);
    Hilbert<?> hilbert = (Hilbert<?>) transform;
    HilbertCurve2D curve = hilbert.curve();
    Assert.assertEquals(10, curve.resolution());
    Assert.assertEquals(-1e10, curve.minX(), 1e-6);
    Assert.assertEquals(-2e10, curve.minY(), 1e-6);
    Assert.assertEquals(10.1, curve.maxX(), 1e-6);
    Assert.assertEquals(20.2, curve.maxY(), 1e-6);
    Transform<?, ?> transform2 = Transforms.fromString(hilbert.toString());
    Assert.assertEquals(transform, transform2);
  }

  @Test
  public void testHilbertEquals() {
    Transform<Object, Long> transform1 = Transforms.hilbert(10, -1e10, -2e10, 10.1, 20.2);
    Transform<Object, Long> transform2 = Transforms.hilbert(10, -1e10, -2e10, 10.1, 20.2);
    Assert.assertEquals(transform1, transform2);
    transform2 = Transforms.hilbert(10, -1e10, -2e10, 10.1, 20.1);
    Assert.assertNotEquals(transform1, transform2);
    transform2 = Transforms.hilbert(9, -1e10, -2e10, 10.1, 20.2);
    Assert.assertNotEquals(transform1, transform2);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testHilbertOnPoints() {
    Transform<?, ?> transform = Transforms.hilbert(10);
    SerializableFunction<Geometry, Long> func =
        (SerializableFunction<Geometry, Long>) transform.bind(Types.GeometryType.get());
    Long value = func.apply(null);
    Assert.assertNull(value);
    value = func.apply(factory.createPoint());
    Assert.assertNull(value);
    value = func.apply(factory.createPoint(new Coordinate(200, 10)));
    Assert.assertNull(value);
    long value0 = func.apply(factory.createPoint(new Coordinate(10, 10)));
    long value1 = func.apply(factory.createPoint(new Coordinate(15, 15)));
    long value2 = func.apply(factory.createPoint(new Coordinate(30, 30)));
    long value3 = func.apply(factory.createPoint(new Coordinate(-60, -30)));
    long value4 = func.apply(factory.createPoint(new Coordinate(-61, -31)));
    long diff01 = Math.abs(value0 - value1);
    long diff12 = Math.abs(value1 - value2);
    long diff03 = Math.abs(value0 - value3);
    long diff34 = Math.abs(value3 - value4);
    Assert.assertTrue(diff34 < diff01);
    Assert.assertTrue(diff01 < diff12);
    Assert.assertTrue(diff12 < diff03);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testHilbertOnPolygons() {
    Transform<?, ?> transform = Transforms.hilbert(10);
    SerializableFunction<Geometry, Long> func =
        (SerializableFunction<Geometry, Long>) transform.bind(Types.GeometryType.get());
    Long value =
        func.apply(
            factory.createPolygon(
                new Coordinate[] {
                  new Coordinate(0, 0),
                  new Coordinate(0, 10),
                  new Coordinate(10, 10),
                  new Coordinate(10, 0),
                  new Coordinate(0, 0)
                }));
    Assert.assertNotNull(value);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testHilbertWithLargerExtent() {
    Transform<?, ?> transform = Transforms.hilbert(10, 0, 0, 1000, 1000);
    SerializableFunction<Geometry, Long> func =
        (SerializableFunction<Geometry, Long>) transform.bind(Types.GeometryType.get());
    Long value = func.apply(factory.createPoint(new Coordinate(300, 200)));
    Assert.assertNotNull(value);
    value = func.apply(factory.createPoint(new Coordinate(-10, -10)));
    Assert.assertNull(value);
  }
}
