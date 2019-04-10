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

import org.apache.spark.sql.execution.streaming.state.StateStoreId
import org.apache.spark.sql.state.StateInformationInCheckpoint.StateInformation
import org.apache.spark.sql.state.StateSchemaExtractor.StateSchemaInfo
import org.apache.spark.sql.types.StructType

/**
 * This class combines [[StateInformation]] and [[StateSchemaInfo]] to provide actual
 * parameters needed for state store read.
 */
object StateStoreReaderOperatorParamExtractor {
  case class StateStoreReaderOperatorParam(
      lastStateVersion: Option[Long],
      opId: Int,
      storeName: String,
      stateSchema: Option[StructType])

  def extract(
      stateInfo: StateInformation,
      schemaInfos: Seq[StateSchemaInfo])
    : Seq[StateStoreReaderOperatorParam] = {

    val lastStateVer = stateInfo.lastCommittedBatchId.map(_ + 1)

    val stInfoGrouped = stateInfo.operators.groupBy(_.opId)
    val schemaInfoGrouped = schemaInfos.groupBy(_.opId)
    stInfoGrouped.flatMap { case (key, value) =>
      if (value.length != 1) {
        throw new IllegalStateException("It should only have one state operator information " +
          "per operation ID")
      }

      value.head.storeNames.map { storeName =>
        val stateSchema: Option[StructType] = {
          if (storeName == StateStoreId.DEFAULT_STORE_NAME) {
            schemaInfoGrouped.get(key).map { infoValue =>
              if (infoValue.length != 1) {
                throw new IllegalStateException("StateSchemaInfo only supports one schema per " +
                  "operator id - which uses DEFAULT_STORE_NAME as store name.")
              }
              val ret = infoValue.head
              new StructType().add("key", ret.keySchema).add("value", ret.valueSchema)
            }
          } else {
            None
          }
        }

        StateStoreReaderOperatorParam(lastStateVer, key, storeName, stateSchema)
      }
    }.toSeq
  }
}
