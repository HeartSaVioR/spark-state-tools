package org.apache.spark.sql.state

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.Update
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
import org.scalatest.{Assertions, BeforeAndAfterAll}

class StateStoreRelationSuite extends StreamTest with BeforeAndAfterAll with Assertions {
  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  import testImplicits._

  test("reading state from simple aggregation - state format version 1") {
    withSQLConf(Seq(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> "1"): _*) {
      withTempDir { tempDir =>
        runStreamingAggregationTestQuery(tempDir.getAbsolutePath)

        val stateKeySchema = new StructType()
          .add("groupKey", IntegerType)

        val stateValueSchema = new StructType()
          .add("groupKey", IntegerType)
          .add("cnt", LongType)
          .add("sum", LongType)
          .add("max", IntegerType)
          .add("min", IntegerType)

        val stateSchema = new StructType()
          .add("key", stateKeySchema)
          .add("value", stateValueSchema)

        val stateReadDf = spark.read
          // FIXME: why it is not registered by default?
          .format("org.apache.spark.sql.state")
          .schema(stateSchema)
          .option(DefaultSource.PARAM_CHECKPOINT_LOCATION,
            new File(tempDir, "state").getAbsolutePath)
          .option(DefaultSource.PARAM_BATCH_ID, "2")
          .option(DefaultSource.PARAM_OPERATOR_ID, "0")
          .load()

        stateReadDf.printSchema()

        checkAnswer(
          stateReadDf
            .selectExpr("key.groupKey AS key_groupKey", "value.groupKey AS value_groupKey",
              "value.cnt AS value_cnt", "value.sum AS value_sum", "value.max AS value_max",
              "value.min AS value_min"),
          Seq(Row(0, 0, 1, 2, 2, 2), Row(1, 1, 2, 6, 3, 3))
        )
      }
    }
  }

  test("reading state from simple aggregation - state format version 2") {
    withSQLConf(Seq(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> "2"): _*) {
      withTempDir { tempDir =>
        runStreamingAggregationTestQuery(tempDir.getAbsolutePath)

        val stateKeySchema = new StructType()
          .add("groupKey", IntegerType)

        // key part not included in state format version 2
        val stateValueSchema = new StructType()
          .add("cnt", LongType)
          .add("sum", LongType)
          .add("max", IntegerType)
          .add("min", IntegerType)

        val stateSchema = new StructType()
          .add("key", stateKeySchema)
          .add("value", stateValueSchema)

        val stateReadDf = spark.read
          // FIXME: why it is not registered by default?
          .format("org.apache.spark.sql.state")
          .schema(stateSchema)
          .option(DefaultSource.PARAM_CHECKPOINT_LOCATION,
            new File(tempDir, "state").getAbsolutePath)
          .option(DefaultSource.PARAM_BATCH_ID, "2")
          .option(DefaultSource.PARAM_OPERATOR_ID, "0")
          .load()

        stateReadDf.printSchema()

        checkAnswer(
          stateReadDf
            .selectExpr("key.groupKey AS key_groupKey", "value.cnt AS value_cnt",
              "value.sum AS value_sum", "value.max AS value_max", "value.min AS value_min"),
          Seq(Row(0, 1, 2, 2, 2), Row(1, 2, 6, 3, 3))
        )
      }
    }
  }

  private def runStreamingAggregationTestQuery(checkpointRoot: String): Unit = {
    import org.apache.spark.sql.functions._

    val inputData = MemoryStream[Int]

    val aggregated = inputData.toDF()
      .selectExpr("value", "value % 2 AS groupKey")
      .groupBy($"groupKey")
      .agg(
        count("*").as("cnt"),
        sum("value").as("sum"),
        max("value").as("max"),
        min("value").as("min")
      )
      .as[(Int, Long, Long, Int, Int)]

    testStream(aggregated, Update)(
      StartStream(checkpointLocation = checkpointRoot),
      AddData(inputData, 3),
      CheckLastBatch((1, 1, 3, 3, 3)),
      AddData(inputData, 3, 2),
      CheckLastBatch((1, 2, 6, 3, 3), (0, 1, 2, 2, 2)),
      StopStream,
      StartStream(checkpointLocation = checkpointRoot),
      AddData(inputData, 3, 2, 1),
      CheckLastBatch((1, 4, 10, 3, 1), (0, 2, 4, 2, 2))
    )
  }
}
