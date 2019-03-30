package org.apache.spark.sql.state

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.state.StateStoreId
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, SchemaRelationProvider}
import org.apache.spark.sql.types.{DataType, StructType}

// TODO: read schema of key and value from metadata of state (requires SPARK-27237)
//  and change SchemaRelationProvider to RelationProvider
class DefaultSource extends DataSourceRegister with SchemaRelationProvider {
  import DefaultSource._

  override def shortName(): String = "state"

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    if (!isValidSchema(schema)) {
      throw new IllegalArgumentException("The fields of schema should be 'key' and 'value', " +
        "and each field should have corresponding fields (they should be a StructType)")
    }

    val keySchema = getSchemaAsDataType(schema, "key").asInstanceOf[StructType]
    val valueSchema = getSchemaAsDataType(schema, "value").asInstanceOf[StructType]

    val checkpointLocation = parameters.get(PARAM_CHECKPOINT_LOCATION) match {
      case Some(cpLocation) => cpLocation

      case None => throw new IllegalArgumentException(s"'$PARAM_CHECKPOINT_LOCATION' must be specified.")
    }

    val batchId = parameters.get(PARAM_BATCH_ID) match {
      case Some(batch) => batch.toInt
      case None => throw new IllegalArgumentException(s"'$PARAM_BATCH_ID' must be specified.")
    }

    val operatorId = parameters.get(PARAM_OPERATOR_ID) match {
      case Some(opId) => opId.toInt
      case None => throw new IllegalArgumentException(s"'$PARAM_OPERATOR_ID' must be specified.")
    }

    val storeName = parameters.get(PARAM_STORE_NAME) match {
      case Some(stName) => stName
      case None => StateStoreId.DEFAULT_STORE_NAME
    }

    new StateStoreRelation(sqlContext.sparkSession, keySchema,
      valueSchema, checkpointLocation, batchId, operatorId,
      storeName, parameters)
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

object DefaultSource {
  val PARAM_CHECKPOINT_LOCATION = "checkpointLocation"
  val PARAM_BATCH_ID = "batchId"
  val PARAM_OPERATOR_ID = "operatorId"
  val PARAM_STORE_NAME = "storeName"
}
