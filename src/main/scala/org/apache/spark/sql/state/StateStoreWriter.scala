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

import java.util.UUID

import org.apache.hadoop.fs.Path

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreId, StateStoreProviderId}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

class StateStoreWriter(
    session: SparkSession,
    data: DataFrame,
    keySchema: StructType,
    valueSchema: StructType,
    stateCheckpointLocation: String,
    version: Int,
    operatorId: Int,
    storeName: String,
    newPartitions: Int) {

  import StateStoreWriter._

  private val storeConf = new StateStoreConf(session.sessionState.conf)

  // A Hadoop Configuration can be about 10 KB, which is pretty big, so broadcast it
  private val hadoopConfBroadcast = session.sparkContext.broadcast(
    new SerializableConfiguration(session.sessionState.newHadoopConf()))

  def write(): Unit = {
    val resolvedCpLocation = {
      val checkpointPath = new Path(stateCheckpointLocation)
      val fs = checkpointPath.getFileSystem(session.sessionState.newHadoopConf())
      if (fs.exists(checkpointPath)) {
        throw new IllegalStateException(s"Checkpoint location should not be exist. " +
          s"Path: $checkpointPath")
      }
      fs.mkdirs(checkpointPath)
      checkpointPath.makeQualified(fs.getUri, fs.getWorkingDirectory).toUri.toString
    }

    // just provide dummy ID since it doesn't matter
    // if it really matters in future, convert it to parameter
    val queryId = UUID.randomUUID()

    // TODO: expand this to cover multi-depth (nested) columns (do we want to cover it?)
    val fullPathsForKeyColumns = keySchema.map(key => Column(s"key.${key.name}"))
    data
      .repartition(newPartitions, fullPathsForKeyColumns: _*)
      .queryExecution
      .toRdd
      .foreachPartition(
        writeFn(resolvedCpLocation, version, operatorId, storeName, keySchema, valueSchema,
          storeConf, hadoopConfBroadcast, queryId))
  }
}

object StateStoreWriter {

  def writeFn(
      resolvedCpLocation: String,
      version: Int,
      operatorId: Int,
      storeName: String,
      keySchema: StructType,
      valueSchema: StructType,
      storeConf: StateStoreConf,
      hadoopConfBroadcast: Broadcast[SerializableConfiguration],
      queryId: UUID): Iterator[InternalRow] => Unit = iter => {
    val taskContext = TaskContext.get()

    val partIdx = taskContext.partitionId()
    val hadoopConf = hadoopConfBroadcast.value.value

    val storeId = StateStoreId(resolvedCpLocation, operatorId, partIdx, storeName)
    val storeProviderId = StateStoreProviderId(storeId, queryId)

    // fill empty state until target version - 1
    (0 until version - 1).map { id =>
      val store = StateStore.get(storeProviderId, keySchema, valueSchema, None, id,
        storeConf, hadoopConf)
      store.commit()
    }

    // all states will be written at version
    val store = StateStore.get(storeProviderId, keySchema, valueSchema, None, version - 1,
      storeConf, hadoopConf)
    iter.foreach { row =>
      store.put(
        row.getStruct(0, keySchema.fields.length).asInstanceOf[UnsafeRow],
        row.getStruct(1, valueSchema.fields.length).asInstanceOf[UnsafeRow]
      )
    }
    store.commit()
  }
}
