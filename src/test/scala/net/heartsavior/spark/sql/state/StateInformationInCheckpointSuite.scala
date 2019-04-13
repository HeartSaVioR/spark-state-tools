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

package net.heartsavior.spark.sql.state

import org.apache.hadoop.fs.Path
import org.scalatest.{Assertions, BeforeAndAfterAll}

import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreId}
import org.apache.spark.sql.hack.SparkSqlHack
import org.apache.spark.sql.internal.SQLConf

class StateInformationInCheckpointSuite
  extends StateStoreTest
  with BeforeAndAfterAll
  with Assertions {

  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  test("Reading checkpoint from streaming aggregation") {
    withTempDir { cpDir =>
      runCompositeKeyStreamingAggregationQuery(cpDir.getAbsolutePath)

      val stateInfo = new StateInformationInCheckpoint(spark)
        .gatherInformation(new Path(cpDir.getAbsolutePath))

      assert(stateInfo.lastCommittedBatchId === Some(2))
      assert(stateInfo.operators.length === 1)

      val operator = stateInfo.operators.head
      assert(operator.opId === 0)
      assert(operator.partitions === SparkSqlHack.sqlConf(spark.sqlContext).numShufflePartitions)
      assert(operator.storeNames === Seq(StateStoreId.DEFAULT_STORE_NAME))

      assert(stateInfo.confs.get(SQLConf.SHUFFLE_PARTITIONS.key) ===
        Some(operator.partitions.toString))
    }
  }

  test("Reading checkpoint from streaming deduplication") {
    withTempDir { cpDir =>
      runStreamingDeduplicationQuery(cpDir.getAbsolutePath)

      val stateInfo = new StateInformationInCheckpoint(spark)
        .gatherInformation(new Path(cpDir.getAbsolutePath))

      assert(stateInfo.lastCommittedBatchId === Some(2))
      assert(stateInfo.operators.length === 1)

      val operator = stateInfo.operators.head
      assert(operator.opId === 0)
      assert(operator.partitions === SparkSqlHack.sqlConf(spark.sqlContext).numShufflePartitions)
      assert(operator.storeNames === Seq(StateStoreId.DEFAULT_STORE_NAME))

      assert(stateInfo.confs.get(SQLConf.SHUFFLE_PARTITIONS.key) ===
        Some(operator.partitions.toString))
    }
  }

  test("Reading checkpoint from streaming join") {
    withTempDir { cpDir =>
      runStreamingJoinQuery(cpDir.getAbsolutePath)

      val stateInfo = new StateInformationInCheckpoint(spark)
        .gatherInformation(new Path(cpDir.getAbsolutePath))

      assert(stateInfo.lastCommittedBatchId === Some(1))
      assert(stateInfo.operators.length === 1)

      val operator = stateInfo.operators.head
      assert(operator.opId === 0)
      assert(operator.partitions === SparkSqlHack.sqlConf(spark.sqlContext).numShufflePartitions)
      // NOTE: this verification couples with implementation details of streaming join
      assert(operator.storeNames.toSet === Set("left-keyToNumValues", "left-keyWithIndexToValue",
        "right-keyToNumValues", "right-keyWithIndexToValue"))

      assert(stateInfo.confs.get(SQLConf.SHUFFLE_PARTITIONS.key) ===
        Some(operator.partitions.toString))
    }
  }

  test("Reading checkpoint from flatMapGroupsWithState") {
    withTempDir { cpDir =>
      runFlatMapGroupsWithStateQuery(cpDir.getAbsolutePath)

      val stateInfo = new StateInformationInCheckpoint(spark)
        .gatherInformation(new Path(cpDir.getAbsolutePath))

      assert(stateInfo.lastCommittedBatchId === Some(1))
      assert(stateInfo.operators.length === 1)

      val operator = stateInfo.operators.head
      assert(operator.opId === 0)
      assert(operator.partitions === SparkSqlHack.sqlConf(spark.sqlContext).numShufflePartitions)
      assert(operator.storeNames === Seq(StateStoreId.DEFAULT_STORE_NAME))

      assert(stateInfo.confs.get(SQLConf.SHUFFLE_PARTITIONS.key) ===
        Some(operator.partitions.toString))
    }
  }
}
