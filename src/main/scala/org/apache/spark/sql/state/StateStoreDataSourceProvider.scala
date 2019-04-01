package org.apache.spark.sql.state

import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.execution.streaming.state.StateStoreId
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, SchemaRelationProvider}
import org.apache.spark.sql.types.{DataType, StructType}

// TODO: read schema of key and value from metadata of state (requires SPARK-27237)
//  and change SchemaRelationProvider to RelationProvider to receive schema optionally
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
      throw new AnalysisException("The fields of schema should be 'key' and 'value', " +
        "and each field should have corresponding fields (they should be a StructType)")
    }

    val keySchema = getSchemaAsDataType(schema, "key").asInstanceOf[StructType]
    val valueSchema = getSchemaAsDataType(schema, "value").asInstanceOf[StructType]

    val checkpointLocation = parameters.get(PARAM_CHECKPOINT_LOCATION) match {
      case Some(cpLocation) => cpLocation
      case None => throw new AnalysisException(s"'$PARAM_CHECKPOINT_LOCATION' must be specified.")
    }

    val version = parameters.get(PARAM_VERSION) match {
      case Some(ver) => ver.toInt
      case None => throw new AnalysisException(s"'$PARAM_VERSION' must be specified.")
    }

    val operatorId = parameters.get(PARAM_OPERATOR_ID) match {
      case Some(opId) => opId.toInt
      case None => throw new AnalysisException(s"'$PARAM_OPERATOR_ID' must be specified.")
    }

    val storeName = parameters.get(PARAM_STORE_NAME) match {
      case Some(stName) => stName
      case None => StateStoreId.DEFAULT_STORE_NAME
    }

    new StateStoreRelation(sqlContext.sparkSession, keySchema,
      valueSchema, checkpointLocation, version, operatorId,
      storeName, parameters)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    mode match {
      case SaveMode.Overwrite | SaveMode.ErrorIfExists => // good
      case _ => throw new AnalysisException(s"Save mode $mode not allowed for state. " +
        s"Allowed save modes are ${SaveMode.Overwrite} and ${SaveMode.ErrorIfExists}.")
    }

    val checkpointLocation = parameters.get(PARAM_CHECKPOINT_LOCATION) match {
      case Some(cpLocation) => cpLocation
      case None => throw new AnalysisException(s"'$PARAM_CHECKPOINT_LOCATION' must be specified.")
    }

    val version = parameters.get(PARAM_VERSION) match {
      case Some(ver) => ver.toInt
      case None => throw new AnalysisException(s"'$PARAM_VERSION' must be specified.")
    }

    val operatorId = parameters.get(PARAM_OPERATOR_ID) match {
      case Some(opId) => opId.toInt
      case None => throw new AnalysisException(s"'$PARAM_OPERATOR_ID' must be specified.")
    }

    val storeName = parameters.get(PARAM_STORE_NAME) match {
      case Some(stName) => stName
      case None => StateStoreId.DEFAULT_STORE_NAME
    }

    val newPartitions = parameters.get(PARAM_NEW_PARTITIONS) match {
      case Some(partitions) => partitions.toInt
      case None => throw new AnalysisException(s"'$PARAM_NEW_PARTITIONS' must be specified.")
    }

    if (!isValidSchema(data.schema)) {
      throw new AnalysisException("The fields of schema should be 'key' and 'value', " +
        "and each field should have corresponding fields (they should be a StructType)")
    }

    val keySchema = getSchemaAsDataType(data.schema, "key").asInstanceOf[StructType]
    val valueSchema = getSchemaAsDataType(data.schema, "value").asInstanceOf[StructType]

    new StateStoreWriter(sqlContext.sparkSession, data, keySchema, valueSchema, checkpointLocation,
      version, operatorId, storeName, newPartitions).write()

    // just return the same as we just update it
    createRelation(sqlContext, parameters, data.schema)
  }

  private def isValidSchema(schema: StructType): Boolean = {
    if (schema.fieldNames.toSeq != Seq("key", "value")) {
      false
    } else if (!getSchemaAsDataType(schema, "key").isInstanceOf[StructType]) {
      false
    } else if (!getSchemaAsDataType(schema, "value").isInstanceOf[StructType]) {
      false
    } else {
      true
    }
  }

  private def getSchemaAsDataType(schema: StructType, fieldName: String): DataType = {
    schema(schema.getFieldIndex(fieldName).get).dataType
  }
}

object StateStoreDataSourceProvider {
  val PARAM_CHECKPOINT_LOCATION = "checkpointLocation"
  val PARAM_VERSION = "version"
  val PARAM_OPERATOR_ID = "operatorId"
  val PARAM_STORE_NAME = "storeName"
  val PARAM_NEW_PARTITIONS = "newPartitions"
}
