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

package org.apache.spark.sql.state

import java.io.File

import org.scalatest.{Assertions, BeforeAndAfterAll}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.Update
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class StateStoreStreamingAggregationWriteSuite
  extends StateStoreTest
  with BeforeAndAfterAll
  with Assertions {

  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    sql("drop table if exists tbl")
  }

  test("rescale state from streaming aggregation - state format version 1") {
    withSQLConf(Seq(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> "1"): _*) {
      withTempCheckpoints { case (oldCpDir, newCpDir) =>
        runLargeDataStreamingAggregationQuery(oldCpDir.getAbsolutePath)

        val operatorId = 0
        val newLastBatchId = 1
        val newShufflePartitions = 20

        val stateSchema = getSchemaForStreamingAggregationQuery(1)

        val stateReadDf = spark.read
          .format("state")
          .schema(stateSchema)
          .option(StateStoreDataSourceProvider.PARAM_CHECKPOINT_LOCATION,
            new File(oldCpDir, "state").getAbsolutePath)
          .option(StateStoreDataSourceProvider.PARAM_VERSION, newLastBatchId + 1)
          .option(StateStoreDataSourceProvider.PARAM_OPERATOR_ID, operatorId)
          .load()

        val expectedRows = stateReadDf.collect()

        // copy all contents except state to new checkpoint root directory
        // adjust number of shuffle partitions in prior to migrate state
        CheckpointUtil.createSavePoint(spark, oldCpDir.getAbsolutePath,
          newCpDir.getAbsolutePath, newLastBatchId,
          newShufflePartitions = Some(newShufflePartitions),
          excludeState = true)

        stateReadDf.write
          .format("state")
          .option(StateStoreDataSourceProvider.PARAM_CHECKPOINT_LOCATION,
            new File(newCpDir, "state").getAbsolutePath)
          .option(StateStoreDataSourceProvider.PARAM_VERSION, newLastBatchId + 1)
          .option(StateStoreDataSourceProvider.PARAM_OPERATOR_ID, operatorId)
          .option(StateStoreDataSourceProvider.PARAM_NEW_PARTITIONS, newShufflePartitions)
          .saveAsTable("tbl")

        // verify write-and-read works
        checkAnswer(spark.sql("select * from tbl"), expectedRows)

        // read again
        val stateReadDf2 = spark.read
          .format("state")
          .schema(stateSchema)
          .option(StateStoreDataSourceProvider.PARAM_CHECKPOINT_LOCATION,
            new File(newCpDir, "state").getAbsolutePath)
          .option(StateStoreDataSourceProvider.PARAM_VERSION, newLastBatchId + 1)
          .option(StateStoreDataSourceProvider.PARAM_OPERATOR_ID, operatorId)
          .load()

        checkAnswer(stateReadDf2, expectedRows)

        verifyContinueRunLargeDataStreamingAggregationQuery(newCpDir.getAbsolutePath,
          newShufflePartitions)
      }
    }
  }

  test("rescale state from streaming aggregation - state format version 2") {
    withSQLConf(Seq(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> "2"): _*) {
      withTempCheckpoints { case (oldCpDir, newCpDir) =>
        runLargeDataStreamingAggregationQuery(oldCpDir.getAbsolutePath)

        val operatorId = 0
        val newLastBatchId = 1
        val newShufflePartitions = 20

        val stateSchema = getSchemaForStreamingAggregationQuery(2)

        val stateReadDf = spark.read
          .format("state")
          .schema(stateSchema)
          .option(StateStoreDataSourceProvider.PARAM_CHECKPOINT_LOCATION,
            new File(oldCpDir, "state").getAbsolutePath)
          .option(StateStoreDataSourceProvider.PARAM_VERSION, newLastBatchId + 1)
          .option(StateStoreDataSourceProvider.PARAM_OPERATOR_ID, operatorId)
          .load()

        val expectedRows = stateReadDf.collect()

        // copy all contents except state to new checkpoint root directory
        // adjust number of shuffle partitions in prior to migrate state
        CheckpointUtil.createSavePoint(spark, oldCpDir.getAbsolutePath,
          newCpDir.getAbsolutePath, newLastBatchId,
          newShufflePartitions = Some(newShufflePartitions),
          excludeState = true)

        stateReadDf.write
          .format("state")
          .option(StateStoreDataSourceProvider.PARAM_CHECKPOINT_LOCATION,
            new File(newCpDir, "state").getAbsolutePath)
          .option(StateStoreDataSourceProvider.PARAM_VERSION, newLastBatchId + 1)
          .option(StateStoreDataSourceProvider.PARAM_OPERATOR_ID, operatorId)
          .option(StateStoreDataSourceProvider.PARAM_NEW_PARTITIONS, newShufflePartitions)
          .saveAsTable("tbl")

        // verify write-and-read works
        checkAnswer(spark.sql("select * from tbl"), expectedRows)

        // read again
        val stateReadDf2 = spark.read
          .format("state")
          .schema(stateSchema)
          .option(StateStoreDataSourceProvider.PARAM_CHECKPOINT_LOCATION,
            new File(newCpDir, "state").getAbsolutePath)
          .option(StateStoreDataSourceProvider.PARAM_VERSION, newLastBatchId + 1)
          .option(StateStoreDataSourceProvider.PARAM_OPERATOR_ID, operatorId)
          .load()

        checkAnswer(stateReadDf2, expectedRows)

        verifyContinueRunLargeDataStreamingAggregationQuery(newCpDir.getAbsolutePath,
          newShufflePartitions)
      }
    }
  }

  test("simple state schema evolution from streaming aggregation - state format version 2") {
    withSQLConf(Seq(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> "2"): _*) {
      withTempCheckpoints { case (oldCpDir, newCpDir) =>
        runLargeDataStreamingAggregationQuery(oldCpDir.getAbsolutePath)

        val operatorId = 0
        val newLastBatchId = 1
        val newShufflePartitions = 20

        val stateSchema = getSchemaForStreamingAggregationQuery(2)

        val stateReadDf = spark.read
          .format("state")
          .schema(stateSchema)
          .option(StateStoreDataSourceProvider.PARAM_CHECKPOINT_LOCATION,
            new File(oldCpDir, "state").getAbsolutePath)
          .option(StateStoreDataSourceProvider.PARAM_VERSION, newLastBatchId + 1)
          .option(StateStoreDataSourceProvider.PARAM_OPERATOR_ID, operatorId)
          .load()

        // rows:
        // (0, 4, 60, 30, 0)
        // (1, 4, 64, 31, 1)
        // (2, 4, 68, 32, 2)
        // (3, 4, 72, 33, 3)
        // (4, 4, 76, 34, 4)
        // (5, 4, 80, 35, 5)
        // (6, 4, 84, 36, 6)
        // (7, 4, 88, 37, 7)
        // (8, 4, 92, 38, 8)
        // (9, 4, 96, 39, 9)

        val evolutionDf = stateReadDf
          // without casting to double it goes to decimal
          // avg() returns decimal if input is decimal type, otherwise double
          .selectExpr("key", "value", "CAST((1.0 * value.sum / value.cnt) AS DOUBLE) AS avg")
          .selectExpr("key", "struct(value.*, avg) AS value")

        logInfo(s"Schema: ${evolutionDf.schema.treeString}")

        // new rows
        val expectedRows = Seq(
          Row(Row(0), Row(4, 60, 30, 0, 15.0d)),
          Row(Row(1), Row(4, 64, 31, 1, 16.0d)),
          Row(Row(2), Row(4, 68, 32, 2, 17.0d)),
          Row(Row(3), Row(4, 72, 33, 3, 18.0d)),
          Row(Row(4), Row(4, 76, 34, 4, 19.0d)),
          Row(Row(5), Row(4, 80, 35, 5, 20.0d)),
          Row(Row(6), Row(4, 84, 36, 6, 21.0d)),
          Row(Row(7), Row(4, 88, 37, 7, 22.0d)),
          Row(Row(8), Row(4, 92, 38, 8, 23.0d)),
          Row(Row(9), Row(4, 96, 39, 9, 24.0d))
        )

        // copy all contents except state to new checkpoint root directory
        // adjust number of shuffle partitions in prior to migrate state
        CheckpointUtil.createSavePoint(spark, oldCpDir.getAbsolutePath,
          newCpDir.getAbsolutePath, newLastBatchId,
          newShufflePartitions = Some(newShufflePartitions),
          excludeState = true)

        evolutionDf.write
          .format("state")
          .option(StateStoreDataSourceProvider.PARAM_CHECKPOINT_LOCATION,
            new File(newCpDir, "state").getAbsolutePath)
          .option(StateStoreDataSourceProvider.PARAM_VERSION, newLastBatchId + 1)
          .option(StateStoreDataSourceProvider.PARAM_OPERATOR_ID, operatorId)
          .option(StateStoreDataSourceProvider.PARAM_NEW_PARTITIONS, newShufflePartitions)
          .saveAsTable("tbl")

        // verify write-and-read works
        checkAnswer(spark.sql("select * from tbl"), expectedRows)

        val newStateSchema = new StructType(stateSchema.fields.map { field =>
          if (field.name == "value") {
            // we've casted to double, so adding double type here
            StructField("value", field.dataType.asInstanceOf[StructType].add("avg", DoubleType))
          } else {
            field
          }
        })

        // read again
        val stateReadDf2 = spark.read
          .format("state")
          .schema(newStateSchema)
          .option(StateStoreDataSourceProvider.PARAM_CHECKPOINT_LOCATION,
            new File(newCpDir, "state").getAbsolutePath)
          .option(StateStoreDataSourceProvider.PARAM_VERSION, "2")
          .option(StateStoreDataSourceProvider.PARAM_OPERATOR_ID, "0")
          .load()

        checkAnswer(stateReadDf2, expectedRows)

        verifyContinueRunLargeDataStreamingAggregationQuery(newCpDir.getAbsolutePath,
          newShufflePartitions)
      }
    }
  }

  private def verifyContinueRunLargeDataStreamingAggregationQuery(
      checkpointRoot: String,
      newShufflePartitions: Int): Unit = {
    import org.apache.spark.sql.functions._
    import testImplicits._

    val inputData = MemoryStream[Int]

    val aggregated = inputData.toDF()
      .selectExpr("value", "value % 10 AS groupKey")
      .groupBy($"groupKey")
      .agg(
        count("*").as("cnt"),
        sum("value").as("sum"),
        max("value").as("max"),
        min("value").as("min")
      )
      .as[(Int, Long, Long, Int, Int)]

    // batch 0
    inputData.addData(0 until 20)
    // batch 1
    inputData.addData(20 until 40)

    // check with more data - leverage full partitions
    testStream(aggregated, Update)(
      StartStream(checkpointLocation = checkpointRoot),
      // batch 2
      AddData(inputData, 0, 1, 2),
      CheckLastBatch(
        (0, 5, 60, 30, 0), // 0, 10, 20, 30, 0
        (1, 5, 65, 31, 1), // 1, 11, 21, 31, 1
        (2, 5, 70, 32, 2) // 2, 12, 22, 32, 2
      ),
      AssertOnQuery { query =>
        val operators = query.lastExecution.executedPlan.collect {
          case p: StateStoreSaveExec => p
        }
        operators.forall(_.stateInfo.get.numPartitions === newShufflePartitions)
      }
    )
  }
}
