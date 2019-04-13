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

package net.heartsavior.spark.sql.state.migration

import net.heartsavior.spark.sql.checkpoint.CheckpointUtil
import net.heartsavior.spark.sql.state.{StateInformationInCheckpoint, StateStoreDataSourceProvider}
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.state.FlatMapGroupsWithStateExecHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * This class enables migration functionality for query using (flat)MapGroupsWithState.
 */
class FlatMapGroupsWithStateMigrator(spark: SparkSession) extends Logging {

  /**
   * Migrate state being written as format version 1 to format version 2.
   *
   * @param checkpointRoot the root path of existing checkpoint
   * @param newCheckpointRoot the root path savepoint with migrated state will be stored
   * @param keySchema key schema of existing state
   * @param valueSchema value schema of existing state
   */
  def convertVersion1To2(
      checkpointRoot: Path,
      newCheckpointRoot: Path,
      keySchema: StructType,
      valueSchema: StructType): Unit = {
    val stateInfo = new StateInformationInCheckpoint(spark).gatherInformation(checkpointRoot)

    val stateVer = stateInfo.confs.getOrElse(
      SQLConf.FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION.key,
      FlatMapGroupsWithStateExecHelper.legacyVersion.toString).toInt

    if (stateVer != 1) {
      throw new IllegalArgumentException("Given checkpoint doesn't use state formation ver. 1 " +
        s"for flatMapGroupsWithState! version: $stateVer")
    }

    val lastCommittedBatchId = stateInfo.lastCommittedBatchId match {
      case Some(bid) => bid
      case None => throw new IllegalArgumentException("No committed batch in given checkpoint.")
    }

    val addConf = Map(SQLConf.FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION.key -> "2")
    CheckpointUtil.createSavePoint(spark, checkpointRoot.toString, newCheckpointRoot.toString,
      lastCommittedBatchId, addConf, excludeState = true)

    val stateSchema = new StructType()
      .add("key", keySchema)
      .add("value", valueSchema)

    val stateVersion = lastCommittedBatchId + 1
    stateInfo.operators.foreach { op =>
      val partitions = op.partitions
      op.storeNames.map { storeName =>
        val stateReadDf = spark.read
          .format("state")
          .schema(stateSchema)
          .option(StateStoreDataSourceProvider.PARAM_CHECKPOINT_LOCATION,
            new Path(checkpointRoot, "state").toString)
          .option(StateStoreDataSourceProvider.PARAM_VERSION, stateVersion)
          .option(StateStoreDataSourceProvider.PARAM_OPERATOR_ID, op.opId)
          .option(StateStoreDataSourceProvider.PARAM_STORE_NAME, storeName)
          .load()

        logInfo(s"Schema of state format 1 (current): ${stateReadDf.schema.treeString}")

        val valueFieldsWithoutTimestamp = valueSchema.filterNot(_.name == "timeoutTimestamp")

        val newValueColumns = valueFieldsWithoutTimestamp.map("value." + _.name).mkString(",")
        val selectExprs = Seq("key", s"struct(struct($newValueColumns) AS groupState, " +
          "CAST(value.timeoutTimestamp AS LONG) AS timeoutTimestamp) AS value")

        val modifiedDf = stateReadDf.selectExpr(selectExprs: _*)

        logInfo(s"Schema of state format 2 (new): ${modifiedDf.schema.treeString}")

        modifiedDf.write
          .format("state")
          .option(StateStoreDataSourceProvider.PARAM_CHECKPOINT_LOCATION,
            new Path(newCheckpointRoot, "state").toString)
          .option(StateStoreDataSourceProvider.PARAM_VERSION, stateVersion)
          .option(StateStoreDataSourceProvider.PARAM_OPERATOR_ID, op.opId)
          .option(StateStoreDataSourceProvider.PARAM_STORE_NAME, storeName)
          .option(StateStoreDataSourceProvider.PARAM_NEW_PARTITIONS, partitions)
          .save

        logInfo(s"Migrated state (opId: ${op.opId}, storeName: ${storeName}, " +
          s"partitions: ${partitions} from format 1 to 2")
      }
    }
  }
}
