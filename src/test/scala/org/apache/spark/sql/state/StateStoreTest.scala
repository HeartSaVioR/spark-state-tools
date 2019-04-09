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
import java.sql.Timestamp

import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.{Append, Update}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, StreamTest, Trigger}
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.util.Utils

trait StateStoreTest extends StreamTest {
  import testImplicits._

  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  protected def withTempCheckpoints(body: (File, File) => Unit) {
    val src = Utils.createTempDir(namePrefix = "streaming.old")
    val tmp = Utils.createTempDir(namePrefix = "streaming.new")
    try {
      body(src, tmp)
    } finally {
      Utils.deleteRecursively(src)
      Utils.deleteRecursively(tmp)
    }
  }

  protected def getSchemaForLargeDataStreamingAggregationQuery(formatVersion: Int): StructType = {
    val stateKeySchema = new StructType()
      .add("groupKey", IntegerType)

    var stateValueSchema = formatVersion match {
      case 1 => new StructType().add("groupKey", IntegerType)
      case 2 => new StructType()
      case v => throw new IllegalArgumentException(s"Not valid format version $v")
    }

    stateValueSchema = stateValueSchema
      .add("cnt", LongType)
      .add("sum", LongType)
      .add("max", IntegerType)
      .add("min", IntegerType)

    new StructType()
      .add("key", stateKeySchema)
      .add("value", stateValueSchema)
  }

  protected def getSchemaForCompositeKeyStreamingAggregationQuery(
      formatVersion: Int): StructType = {
    val stateKeySchema = new StructType()
      .add("groupKey", IntegerType)
      .add("fruit", StringType, nullable = false)

    var stateValueSchema = formatVersion match {
      case 1 =>
        new StructType().add("groupKey", IntegerType).add("fruit", StringType, nullable = false)
      case 2 => new StructType()
      case v => throw new IllegalArgumentException(s"Not valid format version $v")
    }

    stateValueSchema = stateValueSchema
      .add("cnt", LongType, nullable = false)
      .add("sum", LongType)
      .add("max", IntegerType)
      .add("min", IntegerType)

    new StructType()
      .add("key", stateKeySchema)
      .add("value", stateValueSchema)
  }

  protected def runCompositeKeyStreamingAggregationQuery(
      checkpointRoot: String): Unit = {
    import org.apache.spark.sql.functions._

    val inputData = MemoryStream[Int]

    val aggregated = inputData.toDF()
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

    testStream(aggregated, Update)(
      StartStream(checkpointLocation = checkpointRoot),
      // batch 0
      AddData(inputData, 0 to 5: _*),
      CheckLastBatch(
        (0, "Apple", 1, 0, 0, 0),
        (1, "Banana", 1, 1, 1, 1),
        (0, "Strawberry", 1, 2, 2, 2),
        (1, "Apple", 1, 3, 3, 3),
        (0, "Banana", 1, 4, 4, 4),
        (1, "Strawberry", 1, 5, 5, 5)
      ),
      // batch 1
      AddData(inputData, 6 to 10: _*),
      // state also contains (1, "Strawberry", 1, 5, 5, 5) but not updated here
      CheckLastBatch(
        (0, "Apple", 2, 6, 6, 0), // 0, 6
        (1, "Banana", 2, 8, 7, 1), // 1, 7
        (0, "Strawberry", 2, 10, 8, 2), // 2, 8
        (1, "Apple", 2, 12, 9, 3), // 3, 9
        (0, "Banana", 2, 14, 10, 4) // 4, 10
      ),
      StopStream,
      StartStream(checkpointLocation = checkpointRoot),
      // batch 2
      AddData(inputData, 3, 2, 1),
      CheckLastBatch(
        (1, "Banana", 3, 9, 7, 1), // 1, 7, 1
        (0, "Strawberry", 3, 12, 8, 2), // 2, 8, 2
        (1, "Apple", 3, 15, 9, 3) // 3, 9, 3
      )
    )
  }

  protected def runLargeDataStreamingAggregationQuery(
      checkpointRoot: String): Unit = {
    import org.apache.spark.sql.functions._

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

    // check with more data - leverage full partitions
    testStream(aggregated, Update)(
      StartStream(checkpointLocation = checkpointRoot),
      // batch 0
      AddData(inputData, 0 until 20: _*),
      CheckLastBatch(
        (0, 2, 10, 10, 0), // 0, 10
        (1, 2, 12, 11, 1), // 1, 11
        (2, 2, 14, 12, 2), // 2, 12
        (3, 2, 16, 13, 3), // 3, 13
        (4, 2, 18, 14, 4), // 4, 14
        (5, 2, 20, 15, 5), // 5, 15
        (6, 2, 22, 16, 6), // 6, 16
        (7, 2, 24, 17, 7), // 7, 17
        (8, 2, 26, 18, 8), // 8, 18
        (9, 2, 28, 19, 9) // 9, 19
      ),
      // batch 1
      AddData(inputData, 20 until 40: _*),
      CheckLastBatch(
        (0, 4, 60, 30, 0), // 0, 10, 20, 30
        (1, 4, 64, 31, 1), // 1, 11, 21, 31
        (2, 4, 68, 32, 2), // 2, 12, 22, 32
        (3, 4, 72, 33, 3), // 3, 13, 23, 33
        (4, 4, 76, 34, 4), // 4, 14, 24, 34
        (5, 4, 80, 35, 5), // 5, 15, 25, 35
        (6, 4, 84, 36, 6), // 6, 16, 26, 36
        (7, 4, 88, 37, 7), // 7, 17, 27, 37
        (8, 4, 92, 38, 8), // 8, 18, 28, 38
        (9, 4, 96, 39, 9) // 9, 19, 29, 39
      ),
      StopStream,
      StartStream(checkpointLocation = checkpointRoot),
      // batch 2
      AddData(inputData, 0, 1, 2),
      CheckLastBatch(
        (0, 5, 60, 30, 0), // 0, 10, 20, 30, 0
        (1, 5, 65, 31, 1), // 1, 11, 21, 31, 1
        (2, 5, 70, 32, 2) // 2, 12, 22, 32, 2
      )
    )
  }

  protected def runStreamingDeduplicationQuery(
      checkpointRoot: String): Unit = {
    val inputData = MemoryStream[Int]

    val aggregated = inputData.toDF()
      .selectExpr("value", "value % 10 AS groupKey")
      .dropDuplicates(Seq("groupKey"))
      .as[(Int, Int)]

    testStream(aggregated, Update)(
      StartStream(checkpointLocation = checkpointRoot),
      // batch 0
      AddData(inputData, 0 until 20: _*),
      CheckLastBatch(
        (0, 0),
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 4),
        (5, 5),
        (6, 6),
        (7, 7),
        (8, 8),
        (9, 9)
      ),
      // batch 1
      AddData(inputData, 20 until 40: _*),
      // no new update
      CheckLastBatch(),
      StopStream,
      StartStream(checkpointLocation = checkpointRoot),
      // batch 2
      AddData(inputData, 0, 1, 2),
      // no new update
      CheckLastBatch()
    )
  }

  protected def runStreamingJoinQuery(checkpointRoot: String): Unit = {
    import org.apache.spark.sql.functions._

    val inputData = MemoryStream[Int]

    val df = inputData.toDF()
      .selectExpr("value", "CASE value % 2 WHEN 0 THEN 'even' ELSE 'odd' END AS isEven")
    val df2 = df.selectExpr("value AS value2", "iseven AS isEven2")
      .where("value % 3 != 0")

    val joined = df.join(df2, expr("value == value2"))
      .selectExpr("value", "iseven", "value2", "iseven2")
        .as[(Int, String, Int, String)]

    testStream(joined, Append)(
      StartStream(checkpointLocation = checkpointRoot),
      // batch 0
      AddData(inputData, 0 until 5: _*),
      // 0 and 3 don't exist on df2
      CheckLastBatch(
        (1, "odd", 1, "odd"),
        (2, "even", 2, "even"),
        (4, "even", 4, "even")
      ),
      // batch 1
      AddData(inputData, 5 until 10: _*),
      CheckLastBatch(
        (5, "odd", 5, "odd"),
        (7, "odd", 7, "odd"),
        (8, "even", 8, "even")
      )
    )
  }

  protected def runFlatMapGroupsWithStateQuery(checkpointRoot: String): Unit = {
    // scalastyle:off line.size.limit
    // This test code is borrowed from sessionization example of Apache Spark,
    // with modification a bit to run with testStream
    // https://github.com/apache/spark/blob/v2.4.1/examples/src/main/scala/org/apache/spark/examples/sql/streaming/StructuredSessionization.scala
    // scalastyle:on

    val clock = new StreamManualClock

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

    val remapped = sessionUpdates.map(si => (si.id, si.numEvents, si.durationMs, si.expired))

    testStream(remapped, Update)(
      // batch 0
      StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock,
        checkpointLocation = checkpointRoot),
      AddData(inputData, ("hello world", 1L), ("hello scala", 2L)),
      AdvanceManualClock(1 * 1000),
      CheckNewAnswer(
        ("hello", 2, 1000, false),
        ("world", 1, 0, false),
        ("scala", 1, 0, false)
      ),
      // batch 1
      AddData(inputData, ("hello world", 3L), ("hello scala", 4L)),
      AdvanceManualClock(1 * 1000),
      CheckNewAnswer(
        ("hello", 4, 3000, false),
        ("world", 2, 2000, false),
        ("scala", 2, 2000, false)
      )
    )
  }
}

case class Event(sessionId: String, timestamp: Timestamp)

case class SessionInfo(
    numEvents: Int,
    startTimestampMs: Long,
    endTimestampMs: Long) {
  def durationMs: Long = endTimestampMs - startTimestampMs
}

case class SessionUpdate(
    id: String,
    durationMs: Long,
    numEvents: Int,
    expired: Boolean)
