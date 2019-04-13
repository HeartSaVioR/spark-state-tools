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
import org.apache.spark.sql.types.StructType

class StateStoreReaderOperatorParamExtractorSuite
  extends StateStoreTest
    with BeforeAndAfterAll
    with Assertions {

  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  test("combine state info and schema info from streaming aggregation query") {
    withTempDir { cpDir =>
      runCompositeKeyStreamingAggregationQuery(cpDir.getAbsolutePath)

      val stateInfo = new StateInformationInCheckpoint(spark)
        .gatherInformation(new Path(cpDir.getAbsolutePath))
      assert(stateInfo.operators.length === 1)
      // other validation of stateInfo is covered by StateInformationCheckpointSuite

      val query = getCompositeKeyStreamingAggregationQuery

      val schemaInfo = new StateSchemaExtractor(spark).extract(query.toDF)
      assert(schemaInfo.length === 1)
      // other validation of schemaInfo is covered by StateSchemaExtractorSuite

      val opParams = StateStoreReaderOperatorParamExtractor.extract(stateInfo, schemaInfo)

      // expecting only one state operator and only one store name
      assert(opParams.size == 1)
      val opParam = opParams.head
      assert(opParam.lastStateVersion === Some(3))
      assert(opParam.storeName === StateStoreId.DEFAULT_STORE_NAME)
      assert(opParam.stateSchema.isDefined)
      val expectedStateSchema = new StructType()
        .add("key", schemaInfo.head.keySchema)
        .add("value", schemaInfo.head.valueSchema)
      assert(opParam.stateSchema.get === expectedStateSchema)
    }
  }

  test("combine state info and schema info from flatMapGroupsWithState") {
    withTempDir { cpDir =>
      runFlatMapGroupsWithStateQuery(cpDir.getAbsolutePath)

      val stateInfo = new StateInformationInCheckpoint(spark)
        .gatherInformation(new Path(cpDir.getAbsolutePath))
      assert(stateInfo.operators.length === 1)
      // other validation of stateInfo is covered by StateInformationCheckpointSuite

      val query = getFlatMapGroupsWithStateQuery

      val schemaInfo = new StateSchemaExtractor(spark).extract(query.toDF)
      assert(schemaInfo.length === 1)
      // other validation of schemaInfo is covered by StateSchemaExtractorSuite

      val opParams = StateStoreReaderOperatorParamExtractor.extract(stateInfo, schemaInfo)

      // expecting only one state operator and only one store name
      assert(opParams.size == 1)
      val opParam = opParams.head
      assert(opParam.lastStateVersion === Some(2))
      assert(opParam.storeName === StateStoreId.DEFAULT_STORE_NAME)
      assert(opParam.stateSchema.isDefined)
      val expectedStateSchema = new StructType()
        .add("key", schemaInfo.head.keySchema)
        .add("value", schemaInfo.head.valueSchema)
      assert(opParam.stateSchema.get === expectedStateSchema)
    }
  }

  test("combine state info and schema info from streaming join - schema not supported") {
    withTempDir { cpDir =>
      runStreamingJoinQuery(cpDir.getAbsolutePath)

      val stateInfo = new StateInformationInCheckpoint(spark)
        .gatherInformation(new Path(cpDir.getAbsolutePath))
      assert(stateInfo.operators.length === 1)
      // other validation of stateInfo is covered by StateInformationCheckpointSuite

      val query = getStreamingJoinQuery

      val schemaInfo = new StateSchemaExtractor(spark).extract(query.toDF)

      val opParams = StateStoreReaderOperatorParamExtractor.extract(stateInfo, schemaInfo)

      // expecting only one state operator which has 4 store names
      // NOTE: this verification couples with implementation details of streaming join
      assert(opParams.size == 4)
      opParams.forall { opParam =>
        opParam.lastStateVersion.contains(2) && opParam.opId == 0 && opParam.stateSchema.isEmpty
      }
    }
  }
}
