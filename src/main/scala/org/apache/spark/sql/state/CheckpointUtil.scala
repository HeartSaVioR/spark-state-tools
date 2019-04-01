/**
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

import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{CommitLog, OffsetSeqLog}
import org.apache.spark.sql.internal.SQLConf

object CheckpointUtil {

  def createSavePoint(
      sparkSession: SparkSession,
      checkpointRoot: String,
      newCheckpointRoot: String,
      newLastBatchId: Long,
      newShufflePartitions: Option[Int] = None,
      excludeState: Boolean = false): Unit = {
    val hadoopConf = sparkSession.sessionState.newHadoopConf()

    def resolve(cpLocation: String): String = {
      val checkpointPath = new Path(cpLocation)
      val fs = checkpointPath.getFileSystem(hadoopConf)
      checkpointPath.makeQualified(fs.getUri, fs.getWorkingDirectory).toUri.toString
    }

    val src = new Path(resolve(checkpointRoot))
    val srcFs = src.getFileSystem(hadoopConf)
    val dst = new Path(resolve(newCheckpointRoot))
    val dstFs = dst.getFileSystem(hadoopConf)

    if (dstFs.listFiles(dst, false).hasNext) {
      throw new IllegalArgumentException("Destination directory should be empty.")
    }

    dstFs.mkdirs(dst)

    // copy content of src directory to dst directory
    srcFs.listStatus(src).foreach { fs =>
      val path = fs.getPath
      val fileName = path.getName
      if (fileName == "state" && excludeState) {
        // pass
      } else {
        FileUtil.copy(srcFs, path, dstFs, new Path(dst, fileName),
          false, false, hadoopConf)
      }
    }

    val offsetLog = new OffsetSeqLog(sparkSession, new Path(dst, "offsets").toString)
    val logForBatch = offsetLog.get(newLastBatchId) match {
      case Some(log) => log
      case None => throw new IllegalStateException("offset log for batch should be exist")
    }

    val newLogForBatch = newShufflePartitions match {
      case Some(partitions) =>
        logForBatch.copy(
          metadata = logForBatch.metadata.map { md =>
            md.copy(conf = md.conf + (SQLConf.SHUFFLE_PARTITIONS.key -> partitions.toString))
          }
        )

      case None => logForBatch
    }

    // we will restart from last batch + 1: overwrite the last batch with new configuration
    offsetLog.purgeAfter(newLastBatchId - 1)
    offsetLog.add(newLastBatchId, newLogForBatch)

    val commitLog = new CommitLog(sparkSession, new Path(dst, "commits").toString)
    commitLog.purgeAfter(newLastBatchId)

    // state doesn't expose purge mechanism as its interface
    // assuming state would work with overwriting batch files when it replays previous batch
  }
}
