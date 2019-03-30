package org.apache.spark.sql.state

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeRow}
import org.apache.spark.sql.execution.streaming.state.StateStoreId
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType

// TODO: read schema of key and value from metadata of state (requires SPARK-27237)
class StateStoreRelation(
    session: SparkSession,
    keySchema: StructType,
    valueSchema: StructType,
    stateCheckpointLocation: String,
    batchId: Int,
    operatorId: Int,
    storeName: String = StateStoreId.DEFAULT_STORE_NAME,
    sourceOptions: Map[String, String]) extends BaseRelation with TableScan with Logging {

  override def sqlContext: SQLContext = session.sqlContext

  override def schema: StructType = new StructType()
    .add("key", StructType(keySchema.fields), nullable = false)
    .add("value", StructType(valueSchema.fields), nullable = false)

  override def buildScan(): RDD[Row] = {
    val resolvedCpLocation = {
      val checkpointPath = new Path(stateCheckpointLocation)
      val fs = checkpointPath.getFileSystem(sqlContext.sessionState.newHadoopConf())
      fs.mkdirs(checkpointPath)
      checkpointPath.makeQualified(fs.getUri, fs.getWorkingDirectory).toUri.toString
    }

    val rdd = new StateStoreReaderRDD(session, keySchema, valueSchema,
      resolvedCpLocation, batchId, operatorId, storeName)
    rdd.map(StateStoreRelation.unifyStateRowPair(schema))
  }
}

object StateStoreRelation {
  def unifyStateRowPair(schema: StructType)(pair: (UnsafeRow, UnsafeRow)): Row = {
    val row = new GenericInternalRow(2)
    row.update(0, pair._1)
    row.update(1, pair._2)

    val encoder: ExpressionEncoder[Row] = RowEncoder(schema).resolveAndBind()
    encoder.fromRow(row)
  }
}
