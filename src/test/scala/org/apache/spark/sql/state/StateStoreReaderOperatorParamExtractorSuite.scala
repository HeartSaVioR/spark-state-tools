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

import java.sql.Timestamp

import org.apache.hadoop.fs.Path
import org.scalatest.{Assertions, BeforeAndAfterAll}

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreId}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.types.StructType

class StateStoreReaderOperatorParamExtractorSuite
  extends StateStoreTest
    with BeforeAndAfterAll
    with Assertions {

  import testImplicits._
  import org.apache.spark.sql.functions._

  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  test("combine state info and schema info from streaming aggregation query") {
    withTempDir { cpDir =>
      runCompositeKeyStreamingAggregationQuery(cpDir.getAbsolutePath)

      val stateInfo = new StateInformationInCheckpoint(spark)
        .gatherInformation(new Path(cpDir.getAbsolutePath))
      assert(stateInfo.operators.length === 1)
      // other validation of stateInfo is covered by StateInformationCheckpointSuite

      val query = getCompositeKeyStreamingQuery

      val schemaInfo = new StateSchemaExtractor(spark).extract(query.toDF)
      assert(schemaInfo.length === 1)
      // other validation of schemaInfo is covered by StateSchemaExtractorSuite

      val opParams = StateStoreReaderOperatorParamExtractor.extract(stateInfo, schemaInfo)

      // expecting only one state operator and only one store name
      assert(opParams.size == 1)
      val opParam = opParams.head
      assert(opParam.lastStateVersion === Some(3))
      assert(opParam.storeName === StateStoreId.DEFAULT_STORE_NAME)
      assert(opParam.stateSchema.isDefined)
      val expectedStateSchema = new StructType()
        .add("key", schemaInfo.head.keySchema)
        .add("value", schemaInfo.head.valueSchema)
      assert(opParam.stateSchema.get === expectedStateSchema)
    }
  }

  test("combine state info and schema info from flatMapGroupsWithState") {
    withTempDir { cpDir =>
      runFlatMapGroupsWithStateQuery(cpDir.getAbsolutePath)

      val stateInfo = new StateInformationInCheckpoint(spark)
        .gatherInformation(new Path(cpDir.getAbsolutePath))
      assert(stateInfo.operators.length === 1)
      // other validation of stateInfo is covered by StateInformationCheckpointSuite

      val query = getFlatMapGroupsWithStateQuery

      val schemaInfo = new StateSchemaExtractor(spark).extract(query.toDF)
      assert(schemaInfo.length === 1)
      // other validation of schemaInfo is covered by StateSchemaExtractorSuite

      val opParams = StateStoreReaderOperatorParamExtractor.extract(stateInfo, schemaInfo)

      // expecting only one state operator and only one store name
      assert(opParams.size == 1)
      val opParam = opParams.head
      assert(opParam.lastStateVersion === Some(2))
      assert(opParam.storeName === StateStoreId.DEFAULT_STORE_NAME)
      assert(opParam.stateSchema.isDefined)
      val expectedStateSchema = new StructType()
        .add("key", schemaInfo.head.keySchema)
        .add("value", schemaInfo.head.valueSchema)
      assert(opParam.stateSchema.get === expectedStateSchema)
    }
  }

  test("combine state info and schema info from streaming join - schema not supported") {
    withTempDir { cpDir =>
      runStreamingJoinQuery(cpDir.getAbsolutePath)

      val stateInfo = new StateInformationInCheckpoint(spark)
        .gatherInformation(new Path(cpDir.getAbsolutePath))
      assert(stateInfo.operators.length === 1)
      // other validation of stateInfo is covered by StateInformationCheckpointSuite

      val query = getStreamingJoinQuery

      val schemaInfo = new StateSchemaExtractor(spark).extract(query.toDF)

      val opParams = StateStoreReaderOperatorParamExtractor.extract(stateInfo, schemaInfo)

      // expecting only one state operator which has 4 store names
      // NOTE: this verification couples with implementation details of streaming join
      assert(opParams.size == 4)
      opParams.forall { opParam =>
        opParam.lastStateVersion.contains(2) && opParam.opId == 0 && opParam.stateSchema.isEmpty
      }
    }
  }

  private def getCompositeKeyStreamingQuery: Dataset[(Int, String, Long, Long, Int, Int)] = {
    val inputData = MemoryStream[Int]

    // This is borrowed from StateStoreTest, runCompositeKeyStreamingAggregationQuery
    // so we can get schema information from getSchemaForCompositeKeyStreamingAggregationQuery
    inputData.toDF()
      .selectExpr("value", "value % 2 AS groupKey",
        "(CASE value % 3 WHEN 0 THEN 'Apple' WHEN 1 THEN 'Banana' ELSE 'Strawberry' END) AS fruit")
      .groupBy($"groupKey", $"fruit")
      .agg(
        count("*").as("cnt"),
        sum("value").as("sum"),
        max("value").as("max"),
        min("value").as("min")
      )
      .as[(Int, String, Long, Long, Int, Int)]
  }

  private def getFlatMapGroupsWithStateQuery: Dataset[(String, Int, Long)] = {
    val inputData = MemoryStream[(String, Long)]

    val events = inputData.toDF()
      .as[(String, Timestamp)]
      .flatMap { case (line, timestamp) =>
        line.split(" ").map(word => Event(sessionId = word, timestamp))
      }

    val sessionUpdates = events
      .groupByKey(event => event.sessionId)
      .mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.ProcessingTimeTimeout) {

      case (sessionId: String, events: Iterator[Event], state: GroupState[SessionInfo]) =>
        if (state.hasTimedOut) {
          val finalUpdate =
            SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = true)
          state.remove()
          finalUpdate
        } else {
          val timestamps = events.map(_.timestamp.getTime).toSeq
          val updatedSession = if (state.exists) {
            val oldSession = state.get
            SessionInfo(
              oldSession.numEvents + timestamps.size,
              oldSession.startTimestampMs,
              math.max(oldSession.endTimestampMs, timestamps.max))
          } else {
            SessionInfo(timestamps.size, timestamps.min, timestamps.max)
          }
          state.update(updatedSession)

          state.setTimeoutDuration("10 seconds")
          SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = false)
        }
    }

    sessionUpdates.map(si => (si.id, si.numEvents, si.durationMs))
  }

  private def getStreamingJoinQuery: Dataset[(Int, String, Int, String)] = {
    val inputData = MemoryStream[Int]

    val df = inputData.toDF()
      .selectExpr("value", "CASE value % 2 WHEN 0 THEN 'even' ELSE 'odd' END AS isEven")
    val df2 = df.selectExpr("value AS value2", "iseven AS isEven2")
      .where("value % 3 != 0")

    df.join(df2, expr("value == value2"))
      .selectExpr("value", "iseven", "value2", "iseven2")
      .as[(Int, String, Int, String)]
  }
}
