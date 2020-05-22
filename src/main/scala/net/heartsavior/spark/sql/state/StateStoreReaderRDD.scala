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

import java.util.UUID

import scala.util.Try

import net.heartsavior.spark.sql.util.SchemaUtil
import org.apache.hadoop.fs.{Path, PathFilter}

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreId, StateStoreProviderId}
import org.apache.spark.sql.hack.SerializableConfigurationWrapper
import org.apache.spark.sql.types.StructType

class StateStorePartition(
    val partition: Int,
    val queryId: UUID) extends Partition {
  override def index: Int = partition
}

/**
 * An RDD that reads (key, value) pairs of state and provides rows having columns (key, value).
 */
class StateStoreReaderRDD(
    session: SparkSession,
    keySchema: StructType,
    valueSchema: StructType,
    stateCheckpointRootLocation: String,
    batchId: Long,
    operatorId: Long,
    storeName: String)
  extends RDD[Row](session.sparkContext, Nil) {

  private val storeConf = new StateStoreConf(session.sessionState.conf)

  // A Hadoop Configuration can be about 10 KB, which is pretty big, so broadcast it
  private val hadoopConfBroadcastWrapper = new SerializableConfigurationWrapper(session)

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    split match {
      case p: StateStorePartition =>
        val stateStoreId = StateStoreId(stateCheckpointRootLocation, operatorId,
          p.partition, storeName)
        val stateStoreProviderId = StateStoreProviderId(stateStoreId, p.queryId)

        val store = StateStore.get(stateStoreProviderId, keySchema, valueSchema,
          indexOrdinal = None, version = batchId, storeConf = storeConf,
          hadoopConf = hadoopConfBroadcastWrapper.broadcastedConf.value.value)

        val encoder = RowEncoder(SchemaUtil.keyValuePairSchema(keySchema, valueSchema))
          .resolveAndBind()
        val iter = store.iterator().map { pair =>
          val row = new GenericInternalRow(Array(pair.key, pair.value).asInstanceOf[Array[Any]])
          encoder.fromRow(row)
        }

        // close state store provider after using
        StateStore.unload(stateStoreProviderId)

        iter

      case e => throw new IllegalStateException("Expected StateStorePartition but other type of " +
        s"partition passed - $e")
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val fs = stateCheckpointPartitionsLocation.getFileSystem(
      hadoopConfBroadcastWrapper.broadcastedConf.value.value)
    val partitions = fs.listStatus(stateCheckpointPartitionsLocation, new PathFilter() {
      override def accept(path: Path): Boolean = {
        fs.isDirectory(path) && Try(path.getName.toInt).isSuccess && path.getName.toInt >= 0
      }
    })

    if (partitions.headOption.isEmpty) {
      Array.empty[Partition]
    } else {
      // just a dummy query id because we are actually not running streaming query
      val queryId = UUID.randomUUID()

      val partitionsSorted = partitions.sortBy(fs => fs.getPath.getName.toInt)
      val partitionNums = partitionsSorted.map(_.getPath.getName.toInt)
      // assuming no same number - they're directories hence no same name
      val head = partitionNums.head
      val tail = partitionNums(partitionNums.length - 1)
      assert(head == 0, "Partition should start with 0")
      assert((tail - head + 1) == partitionNums.length,
        s"No continuous partitions in state: $partitionNums")

      partitionNums.map(pn => new StateStorePartition(pn, queryId)).toArray
    }
  }

  def stateCheckpointPartitionsLocation: Path = {
    new Path(stateCheckpointRootLocation, s"$operatorId")
  }

  def stateCheckpointLocation(partitionId: Int): Path = {
    val partitionsLocation = stateCheckpointPartitionsLocation
    if (storeName == StateStoreId.DEFAULT_STORE_NAME) {
      // For reading state store data that was generated before store names were used (Spark <= 2.2)
      new Path(partitionsLocation, s"$partitionId")
    } else {
      new Path(partitionsLocation, s"$partitionId/$storeName")
    }
  }
}
