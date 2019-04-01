package org.apache.spark.sql.state

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
import org.scalatest.{Assertions, BeforeAndAfterAll}

class StateStoreRelationSuite extends StateStoreTest with BeforeAndAfterAll with Assertions {
  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  test("reading state from simple aggregation - state format version 1") {
    withSQLConf(Seq(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> "1"): _*) {
      withTempDir { tempDir =>
        runLargeDataStreamingAggregationQuery(tempDir.getAbsolutePath)

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
          Seq(
            Row(0, 0, 4, 60, 30, 0), // 0, 10, 20, 30
            Row(1, 1, 4, 64, 31, 1), // 1, 11, 21, 31
            Row(2, 2, 4, 68, 32, 2), // 2, 12, 22, 32
            Row(3, 3, 4, 72, 33, 3), // 3, 13, 23, 33
            Row(4, 4, 4, 76, 34, 4), // 4, 14, 24, 34
            Row(5, 5, 4, 80, 35, 5), // 5, 15, 25, 35
            Row(6, 6, 4, 84, 36, 6), // 6, 16, 26, 36
            Row(7, 7, 4, 88, 37, 7), // 7, 17, 27, 37
            Row(8, 8, 4, 92, 38, 8), // 8, 18, 28, 38
            Row(9, 9, 4, 96, 39, 9) // 9, 19, 29, 39
          )
        )
      }
    }
  }

  test("reading state from simple aggregation - state format version 2") {
    withSQLConf(Seq(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> "2"): _*) {
      withTempDir { tempDir =>
        runLargeDataStreamingAggregationQuery(tempDir.getAbsolutePath)

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
          Seq(
            Row(0, 4, 60, 30, 0), // 0, 10, 20, 30
            Row(1, 4, 64, 31, 1), // 1, 11, 21, 31
            Row(2, 4, 68, 32, 2), // 2, 12, 22, 32
            Row(3, 4, 72, 33, 3), // 3, 13, 23, 33
            Row(4, 4, 76, 34, 4), // 4, 14, 24, 34
            Row(5, 4, 80, 35, 5), // 5, 15, 25, 35
            Row(6, 4, 84, 36, 6), // 6, 16, 26, 36
            Row(7, 4, 88, 37, 7), // 7, 17, 27, 37
            Row(8, 4, 92, 38, 8), // 8, 18, 28, 38
            Row(9, 4, 96, 39, 9) // 9, 19, 29, 39
          )
        )
      }
    }
  }
}
