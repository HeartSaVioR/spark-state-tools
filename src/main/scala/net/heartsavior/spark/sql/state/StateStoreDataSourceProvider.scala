/*
 * Copyright 2019 Jungtaek Lim "<kabhwan@gmail.com>"
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.heartsavior.spark.sql.state

import net.heartsavior.spark.sql.util.SchemaUtil

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.state.StateStoreId
import org.apache.spark.sql.hack.SparkSqlHack
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

// TODO: read schema of key and value from metadata of state (requires SPARK-27237)
//  and change SchemaRelationProvider to RelationProvider to receive schema optionally
/**
 * Data Source Provider for state store to enable read to/write from state.
 */
class StateStoreDataSourceProvider
  extends DataSourceRegister
  with SchemaRelationProvider
  with CreatableRelationProvider {

  import StateStoreDataSourceProvider._

  override def shortName(): String = "state"

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    if (!isValidSchema(schema)) {
      throw SparkSqlHack.analysisException("The fields of schema should be 'key' and 'value', " +
        "and each field should have corresponding fields (they should be a StructType)")
    }

    val keySchema = SchemaUtil.getSchemaAsDataType(schema, "key").asInstanceOf[StructType]
    val valueSchema = SchemaUtil.getSchemaAsDataType(schema, "value").asInstanceOf[StructType]

    val checkpointLocation = parameters.get(PARAM_CHECKPOINT_LOCATION) match {
      case Some(cpLocation) => cpLocation
      case None => throw SparkSqlHack.analysisException(
        s"'$PARAM_CHECKPOINT_LOCATION' must be specified.")
    }

    val version = parameters.get(PARAM_VERSION) match {
      case Some(ver) => ver.toInt
      case None => throw SparkSqlHack.analysisException(s"'$PARAM_VERSION' must be specified.")
    }

    val operatorId = parameters.get(PARAM_OPERATOR_ID) match {
      case Some(opId) => opId.toInt
      case None => throw SparkSqlHack.analysisException(s"'$PARAM_OPERATOR_ID' must be specified.")
    }

    val storeName = parameters.get(PARAM_STORE_NAME) match {
      case Some(stName) => stName
      case None => StateStoreId.DEFAULT_STORE_NAME
    }

    new StateStoreRelation(sqlContext.sparkSession, keySchema,
      valueSchema, checkpointLocation, version, operatorId,
      storeName)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    mode match {
      case SaveMode.Overwrite | SaveMode.ErrorIfExists => // good
      case _ => throw SparkSqlHack.analysisException(s"Save mode $mode not allowed for state. " +
        s"Allowed save modes are ${SaveMode.Overwrite} and ${SaveMode.ErrorIfExists}.")
    }

    val checkpointLocation = parameters.get(PARAM_CHECKPOINT_LOCATION) match {
      case Some(cpLocation) => cpLocation
      case None => throw SparkSqlHack.analysisException(
        s"'$PARAM_CHECKPOINT_LOCATION' must be specified.")
    }

    val version = parameters.get(PARAM_VERSION) match {
      case Some(ver) => ver.toInt
      case None => throw SparkSqlHack.analysisException(s"'$PARAM_VERSION' must be specified.")
    }

    val operatorId = parameters.get(PARAM_OPERATOR_ID) match {
      case Some(opId) => opId.toInt
      case None => throw SparkSqlHack.analysisException(s"'$PARAM_OPERATOR_ID' must be specified.")
    }

    val storeName = parameters.get(PARAM_STORE_NAME) match {
      case Some(stName) => stName
      case None => StateStoreId.DEFAULT_STORE_NAME
    }

    val newPartitions = parameters.get(PARAM_NEW_PARTITIONS) match {
      case Some(partitions) => partitions.toInt
      case None => throw SparkSqlHack.analysisException(
        s"'$PARAM_NEW_PARTITIONS' must be specified.")
    }

    if (!isValidSchema(data.schema)) {
      throw SparkSqlHack.analysisException("The fields of schema should be 'key' and 'value', " +
        "and each field should have corresponding fields (they should be a StructType)")
    }

    val keySchema = SchemaUtil.getSchemaAsDataType(data.schema, "key").asInstanceOf[StructType]
    val valueSchema = SchemaUtil.getSchemaAsDataType(data.schema, "value").asInstanceOf[StructType]

    new StateStoreWriter(sqlContext.sparkSession, data, keySchema, valueSchema, checkpointLocation,
      version, operatorId, storeName, newPartitions).write()

    // just return the same as we just update it
    createRelation(sqlContext, parameters, data.schema)
  }

  private def isValidSchema(schema: StructType): Boolean = {
    if (schema.fieldNames.toSeq != Seq("key", "value")) {
      false
    } else if (!SchemaUtil.getSchemaAsDataType(schema, "key").isInstanceOf[StructType]) {
      false
    } else if (!SchemaUtil.getSchemaAsDataType(schema, "value").isInstanceOf[StructType]) {
      false
    } else {
      true
    }
  }
}

object StateStoreDataSourceProvider {
  val PARAM_CHECKPOINT_LOCATION = "checkpointLocation"
  val PARAM_VERSION = "version"
  val PARAM_OPERATOR_ID = "operatorId"
  val PARAM_STORE_NAME = "storeName"
  val PARAM_NEW_PARTITIONS = "newPartitions"
}
