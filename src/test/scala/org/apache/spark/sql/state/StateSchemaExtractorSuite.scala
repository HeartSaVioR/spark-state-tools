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

import org.scalatest.{Assertions, BeforeAndAfterAll}

import org.apache.spark.sql.{Dataset, Encoders, SchemaUtil}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

class StateSchemaExtractorSuite
  extends StateStoreTest
    with BeforeAndAfterAll
    with Assertions {

  import testImplicits._

  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  Seq(1, 2).foreach { ver =>
    test(s"extract schema from streaming aggregation query - state format v$ver") {
      withSQLConf(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> ver.toString) {
        // This is borrowed from StateStoreTest, runCompositeKeyStreamingAggregationQuery
        // so we can get schema information from getSchemaForCompositeKeyStreamingAggregationQuery
        val aggregated = getCompositeKeyStreamingQuery

        val stateSchema = getSchemaForCompositeKeyStreamingAggregationQuery(ver)
        val expectedKeySchema = SchemaUtil.getSchemaAsDataType(stateSchema, "key")
          .asInstanceOf[StructType]
        val expectedValueSchema = SchemaUtil.getSchemaAsDataType(stateSchema, "value")
          .asInstanceOf[StructType]

        val schemaInfos = new StateSchemaExtractor(spark).extract(aggregated.toDF())
        assert(schemaInfos.length === 1)
        val schemaInfo = schemaInfos.head
        assert(schemaInfo.opId === 0)
        assert(schemaInfo.formatVersion === ver)
        assert(compareSchemaWithoutName(schemaInfo.keySchema, expectedKeySchema),
          s"Even without column names, ${schemaInfo.keySchema} did not equal $expectedKeySchema")
        assert(compareSchemaWithoutName(schemaInfo.valueSchema, expectedValueSchema),
          s"Even without column names, ${schemaInfo.valueSchema} did not equal " +
            s"$expectedValueSchema")
      }
    }
  }

  Seq(1, 2).foreach { ver =>
    test(s"extract schema from flatMapGroupsWithState query - state format v$ver") {
      withSQLConf(SQLConf.FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION.key -> ver.toString) {
        // This is borrowed from StateStoreTest, runFlatMapGroupsWithStateQuery
        val aggregated = getFlatMapGroupsWithStateQuery

        val expectedKeySchema = new StructType().add("value", StringType, nullable = true)

        val expectedValueSchema = if (ver == 1) {
          Encoders.product[SessionInfo].schema
            .add("timeoutTimestamp", IntegerType, nullable = false)
        } else {
          // ver == 2
          new StructType()
            .add("groupState", Encoders.product[SessionInfo].schema)
            .add("timeoutTimestamp", LongType, nullable = false)
        }

        val schemaInfos = new StateSchemaExtractor(spark).extract(aggregated.toDF())
        assert(schemaInfos.length === 1)
        val schemaInfo = schemaInfos.head
        assert(schemaInfo.opId === 0)
        assert(schemaInfo.formatVersion === ver)

        assert(compareSchemaWithoutName(schemaInfo.keySchema, expectedKeySchema),
          s"Even without column names, ${schemaInfo.keySchema} did not equal $expectedKeySchema")
        assert(compareSchemaWithoutName(schemaInfo.valueSchema, expectedValueSchema),
          s"Even without column names, ${schemaInfo.valueSchema} did not equal " +
            s"$expectedValueSchema")
      }
    }
  }

  private def getCompositeKeyStreamingQuery: Dataset[(Int, String, Long, Long, Int, Int)] = {
    import org.apache.spark.sql.functions._

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

  private def compareSchemaWithoutName(s1: StructType, s2: StructType): Boolean = {
    if (s1.length != s2.length) {
      false
    } else {
      s1.zip(s2).forall { case (column1, column2) =>
        column1.dataType == column2.dataType && column1.nullable == column2.nullable
      }
    }
  }
}
