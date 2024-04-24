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

package org.apache.spark.sql.execution.datasources.v2.havasu

import org.apache.iceberg.relocated.com.google.common.base.Preconditions
import org.apache.iceberg.spark.source.SparkTable
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Types.GeometryType
import org.apache.iceberg.types.havasu.GeometryEncoding
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

case class SetGeometryFieldsExec(
  catalog: TableCatalog,
  ident: Identifier,
  fields: Seq[(String, String)]
) extends LeafV2CommandExec {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    catalog.loadTable(ident) match {
      case iceberg: SparkTable =>
        val schema = iceberg.table.schema
        var updateSchema = iceberg.table.updateSchema()
        for ((name, encodingName) <- fields) {
          val field = schema.findField(name)
          Preconditions.checkArgument(field != null,
            "Cannot complete set geometry fields operation: field %s not found", name)
          if (encodingName != "none") {
            // promote to geometry type
            val encoding = GeometryEncoding.fromName(encodingName)
            Preconditions.checkArgument(encoding.physicalTypeId() == field.`type`().typeId(),
              s"Cannot complete set geometry fields operation: field %s is not of type ${encoding.physicalType()}",
              name)
            updateSchema = updateSchema.updateColumn(name, GeometryType.get(encoding))
          } else {
            // demote from geometry type to underlying physical type
            Preconditions.checkArgument(field.`type`().typeId() == Type.TypeID.GEOMETRY,
              "Cannot demote field %s to non-geometry type", name)
            val physicalType = field.`type`().asInstanceOf[GeometryType].encoding().physicalType()
            updateSchema = updateSchema.updateColumn(name, physicalType.asPrimitiveType())
          }
        }
        updateSchema.commit()
      case table =>
        throw new UnsupportedOperationException(s"Cannot drop identifier fields in non-Iceberg table: $table")
    }

    Nil
  }

  override def simpleString(maxFields: Int): String = {
    val fieldsString = fields.map { case (field, geometryType) => s"$field: $geometryType" }.mkString(", ")
    s"SetGeometryFields ${catalog.name}.${ident.quoted} ($fieldsString)"
  }
}
