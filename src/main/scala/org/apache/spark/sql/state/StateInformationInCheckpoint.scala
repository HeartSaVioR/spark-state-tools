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

import scala.util.Try

import org.apache.hadoop.fs.{FileStatus, Path, PathFilter}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{CommitLog, OffsetSeqLog}
import org.apache.spark.sql.execution.streaming.state.StateStoreId
import org.apache.spark.sql.util.HadoopPathUtil

/**
 * This class enables retrieving
 * [[org.apache.spark.sql.state.StateInformationInCheckpoint.StateInformation]]
 * via reading checkpoint.
 */
class StateInformationInCheckpoint(spark: SparkSession) {
  import org.apache.spark.sql.state.StateInformationInCheckpoint._

  val hadoopConf = spark.sessionState.newHadoopConf()

  def gatherInformation(checkpointPath: Path): StateInformation = {
    val offsetSeq = new OffsetSeqLog(spark, new Path(checkpointPath, "offsets").toString)
    val confMap: Map[String, String] = offsetSeq.getLatest() match {
      case Some((_, offset)) => offset.metadata match {
        case Some(md) => md.conf
        case None => Map.empty[String, String]
      }
      case None => Map.empty[String, String]
    }

    val commitLog = new CommitLog(spark, new Path(checkpointPath, "commits").toString)
    val lastCommittedBatchId = commitLog.getLatest() match {
      case Some((lastId, _)) => lastId
      case None => -1
    }

    if (lastCommittedBatchId < 0) {
      return StateInformation(None, Seq.empty[StateOperatorInformation], confMap)
    }

    val fs = checkpointPath.getFileSystem(hadoopConf)
    val numericDirectories = new PathFilter() {
      override def accept(path: Path): Boolean = {
        fs.isDirectory(path) && Try(path.getName.toInt).isSuccess && path.getName.toInt >= 0
      }
    }

    val statePath = new Path(checkpointPath, "state")
    val operatorDirs = fs.listStatus(statePath, numericDirectories)

    val opInfos = operatorDirs.map { operatorDir =>
      val opPath = operatorDir.getPath
      val opId = opPath.getName.toInt

      val partitions = fs.listStatus(opPath, numericDirectories)
      if (partitions.nonEmpty) {
        validateCorrectPartitions(partitions)

        // assuming information is same across partitions
        val partitionDir = partitions.head

        val statuses = fs.listStatus(partitionDir.getPath)
        val dirs = statuses.filter(status => fs.isDirectory(status.getPath))
        val storeNames = if (dirs.nonEmpty) {
          dirs.map(_.getPath.getName).toList
        } else {
          // assuming default store name
          List(StateStoreId.DEFAULT_STORE_NAME)
        }

        StateOperatorInformation(opId, partitions.length, storeNames)
      } else {
        StateOperatorInformation(opId, 0, Seq.empty)
      }
    }

    StateInformation(Some(lastCommittedBatchId), opInfos, confMap)
  }

  private def validateCorrectPartitions(partitions: Array[FileStatus]): Unit = {
    val partitionsSorted = partitions.sortBy(fs => fs.getPath.getName.toInt)
    val partitionNums = partitionsSorted.map(_.getPath.getName.toInt)
    // assuming no same number - they're directories hence no same name
    val head = partitionNums.head
    val tail = partitionNums(partitionNums.length - 1)
    assert(head == 0, "Partition should start with 0")
    assert((tail - head + 1) == partitionNums.length,
      s"No continuous partitions in state: $partitionNums")
  }
}

object StateInformationInCheckpoint {

  case class StateOperatorInformation(opId: Int, partitions: Int, storeNames: Seq[String])
  case class StateInformation(
      lastCommittedBatchId: Option[Long],
      operators: Seq[StateOperatorInformation],
      confs: Map[String, String])

  // scalastyle:off println
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StateInformationInCheckpoint")
      .getOrCreate()

    if (args.length < 1) {
      System.err.println("Usage: StateInformationInCheckpoint [checkpoint path]")
      sys.exit(1)
    }

    val checkpointRoot = args(0)

    println(s"Checkpoint path: $checkpointRoot")

    val hadoopConf = spark.sessionState.newHadoopConf()
    val checkpointPath = new Path(HadoopPathUtil.resolve(hadoopConf, checkpointRoot))
    val fs = checkpointPath.getFileSystem(hadoopConf)

    if (!fs.exists(checkpointPath) || !fs.isDirectory(checkpointPath)) {
      System.err.println("Checkpoint path doesn't exist or not a directory.")
      sys.exit(2)
    }

    val stateInfo = new StateInformationInCheckpoint(spark).gatherInformation(checkpointPath)

    stateInfo.lastCommittedBatchId match {
      case Some(lastId) =>
        println(s"Last committed batch ID: $lastId")
        stateInfo.operators.foreach { op =>
          println(s"Operator ID: ${op.opId}, partitions: ${op.partitions}, " +
            s"storeNames: ${op.storeNames}")
        }

      case None => println("No batch has been committed.")
    }
  }
  // scalastyle:on println
}
