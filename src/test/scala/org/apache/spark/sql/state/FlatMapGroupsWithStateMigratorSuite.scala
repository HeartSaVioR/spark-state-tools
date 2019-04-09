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

import org.apache.hadoop.fs.Path
import org.scalatest.{Assertions, BeforeAndAfterAll}
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.Update
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.state.migration.FlatMapGroupsWithStateMigrator
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, Trigger}
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

class FlatMapGroupsWithStateMigratorSuite
  extends StateStoreTest
    with BeforeAndAfterAll
    with Assertions {

  override val streamingTimeout: Span = 30.seconds

  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  test("migrate flatMapGroupsWithState state format version 1 to 2") {
    withTempCheckpoints { case (oldCpDir, newCpDir) =>
      val oldCpPath = new Path(oldCpDir.getAbsolutePath)
      val newCpPath = new Path(newCpDir.getAbsolutePath)

      // run flatMapGroupsWithState query to state format version 1
      withSQLConf(SQLConf.FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION.key -> "1") {
        runFlatMapGroupsWithStateQuery(oldCpDir.getAbsolutePath)
      }

      val keySchema = new StructType().add("value", StringType, nullable = true)

      val valueSchema = Encoders.product[SessionInfo].schema
        .add("timeoutTimestamp", IntegerType, nullable = false)

      val migrator = new FlatMapGroupsWithStateMigrator(spark)
      migrator.convertVersion1To2(oldCpPath, newCpPath, keySchema, valueSchema)

      val newStateInfo = new StateInformationInCheckpoint(spark).gatherInformation(newCpPath)
      assert(newStateInfo.lastCommittedBatchId.isDefined,
        "The checkpoint directory should contain committed batch!")

      // check whether it's running well with new checkpoint

      // read state with new expected state schema (state format version 2)
      val newValueSchema = new StructType()
        .add("groupState", Encoders.product[SessionInfo].schema)
        .add("timeoutTimestamp", LongType, nullable = false)

      val newStateSchema = new StructType()
        .add("key", keySchema)
        .add("value", newValueSchema)

      // we assume operator id = 0, store_name = default
      val stateReadDf = spark.read
        .format("state")
        .schema(newStateSchema)
        .option(StateStoreDataSourceProvider.PARAM_CHECKPOINT_LOCATION,
          new File(newCpDir, "state").getAbsolutePath)
        .option(StateStoreDataSourceProvider.PARAM_VERSION,
          newStateInfo.lastCommittedBatchId.get + 1)
        .option(StateStoreDataSourceProvider.PARAM_OPERATOR_ID, 0)
        .load()

      checkAnswer(
        stateReadDf
          .selectExpr("key.value AS key_value", "value.groupState.numEvents AS value_numEvents",
            "value.groupState.startTimestampMs AS value_startTimestampMs",
            "value.groupState.endTimestampMs AS value_endTimestampMs",
            "value.timeoutTimestamp AS value_timeoutTimestamp"),
        Seq(
          Row("hello", 4, 1000, 4000, 12000),
          Row("world", 2, 1000, 3000, 12000),
          Row("scala", 2, 2000, 4000, 12000)
        )
      )

      // rerun streaming query from migrated checkpoint
      verifyFlatMapGroupsWithStateQuery(newCpPath.toString)
    }
  }

  private def verifyFlatMapGroupsWithStateQuery(checkpointRoot: String): Unit = {
    // scalastyle:off line.size.limit
    // This test code is borrowed from sessionization example of Apache Spark,
    // with modification a bit to run with testStream
    // https://github.com/apache/spark/blob/v2.4.1/examples/src/main/scala/org/apache/spark/examples/sql/streaming/StructuredSessionization.scala
    // scalastyle:on
    import testImplicits._

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

    // batch 0
    inputData.addData(("hello world", 1L), ("hello scala", 2L))
    clock.advance(1 * 1000)

    // batch 1
    inputData.addData(("hello world", 3L), ("hello scala", 4L))
    clock.advance(1 * 1000)

    testStream(remapped, Update)(
      StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock,
        checkpointLocation = checkpointRoot),

      // batch 2
      AddData(inputData, ("spark scala", 20L)),
      AdvanceManualClock(15 * 1000),
      CheckNewAnswer(
        ("hello", 4, 3000, true),
        ("world", 2, 2000, true),
        ("spark", 1, 0, false),
        ("scala", 3, 18000, false)
      ),
      // batch 3
      AddData(inputData, ("hello world", 30L), ("hello spark scala", 32L)),
      AdvanceManualClock(15 * 1000),
      CheckNewAnswer(
        ("hello", 2, 2000, false),
        ("world", 1, 0, false),
        ("spark", 2, 12000, false),
        ("scala", 4, 30000, false)
      )
    )
  }
}
