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

package org.apache.spark.sql.hack

import java.io.File

import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.streaming.{FlatMapGroupsWithStateExec, StateStoreSaveExec}
import org.apache.spark.sql.execution.streaming.state.FlatMapGroupsWithStateExecHelper.StateManager
import org.apache.spark.sql.execution.streaming.state.StreamingAggregationStateManager
import org.apache.spark.sql.internal.{SessionState, SQLConf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

object SparkSqlHack {
  def getFieldIndex(schema: StructType, fieldName: String): Option[Int] = {
    schema.getFieldIndex(fieldName)
  }

  def sessionState(sqlContext: SQLContext): SessionState = {
    sqlContext.sessionState
  }

  def sqlConf(sqlContext: SQLContext): SQLConf = {
    sqlContext.conf
  }

  def logicalPlan(query: DataFrame): LogicalPlan = query.logicalPlan

  def stateManager(exec: StateStoreSaveExec): StreamingAggregationStateManager = {
    exec.stateManager
  }

  def stateManager(exec: FlatMapGroupsWithStateExec): StateManager = {
    exec.stateManager
  }

  def analysisException(message: String): AnalysisException = {
    new AnalysisException(message)
  }

  def createTempDir(
      root: String = System.getProperty("java.io.tmpdir"),
      namePrefix: String = "spark"): File = {
    Utils.createTempDir(root, namePrefix)
  }

  def deleteRecursively(file: File): Unit = Utils.deleteRecursively(file)
}
