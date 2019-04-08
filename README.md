# Spark State Tools 

[![CircleCI](https://circleci.com/gh/HeartSaVioR/spark-state-tool/tree/master.svg?style=svg)](https://circleci.com/gh/HeartSaVioR/spark-state-tool/tree/master)

Spark State Tools provides features about offline manipulation of Structured Streaming state on existing query.

The features we provide as of now are:

* Read state as batch source of Spark SQL
* Write DataFrame to state as batch sink of Spark SQL
  * Repartitioning is supported while writing state
* Create savepoint from existing checkpoint of Structured Streaming query
  * You can pick specific batch (if it exists on metadata) to create savepoint.
  * With feature of writing state, you can achieve rescaling state, simple schema evolution, etc.
* Show state operator information in checkpoint which you'll need to provide to enjoy above features
* Migrate state format from old to new
  * Currently, migrating Streaming Aggregation from ver 1 to 2 is supported.

As this project leverages Spark Structured Streaming's interfaces, and doesn't deal with internal
(e.g. the structure of state file for HDFS state store), the performance may be suboptimal.

For now, from the most parts, only states from Streaming Aggregation query (`groupBy().agg()`) are supported.

## Disclaimer

This is something more of a proof of concept implementation, might not be something for production ready.
When you deal with writing state, you may want to backup your checkpoint with CheckpointUtil and try doing it with savepoint.

The project is intended to deal with offline state, not against state which streaming query is running.
Actually it can be possible, but state store provider in running query can purge old batches, which would produce error on here.

## Supported versions

Spark 2.4.x is supported: it only means you should link Spark 2.4.x when using this tool. That state formats across the Spark 2.x versions are supported.

## How to use

FIXME: Adding artifact to your project will be described here when the artifact is published to Maven central.

First of all, you may want to get state and last batch information to provide them as parameters.
You can get it from `StateInformationInCheckpoint`, whether calling from your codebase or running with `spark-submit`.
Here we assume you have artifact jar of spark-state-tool and you want to run it from cli (leveraging `spark-submit`).

```text
<spark_path>/bin/spark-submit --master "local[*]" \
--class org.apache.spark.sql.state.StateInformationInCheckpoint \
spark-state-tool-0.0.1-SNAPSHOT.jar <checkpoint_root_path>
```

The command line will provide checkpoint information like below:

```text
Last committed batch ID: 2
Operator ID: 0, partitions: 5, storeNames: List(default)
```

This output means the query has batch ID 2 as last committed (NOTE: corresponding state version is 3, not 2), and
there's only one stateful operator which has ID as 0, and 5 partitions, and there's also only one kind of store named "default".

To read state from your existing query, you can start your batch query like:

```scala
val stateKeySchema = new StructType()
  .add("groupKey", IntegerType)

val stateValueSchema = new StructType()
  .add("cnt", LongType)
  .add("sum", LongType)
  .add("max", IntegerType)
  .add("min", IntegerType)

val stateFormat = new StructType()
  .add("key", stateKeySchema)
  .add("value", stateValueSchema)

val operatorId = 0
val batchId = 1 // the version of state for the output of batch is batchId + 1

// Here we assume 'spark' as SparkSession
val stateReadDf = spark.read
  .format("state")
  .schema(stateSchema)
  .option(StateStoreDataSourceProvider.PARAM_CHECKPOINT_LOCATION,
    new Path(checkpointRoot, "state").getAbsolutePath)
  .option(StateStoreDataSourceProvider.PARAM_VERSION, batchId + 1)
  .option(StateStoreDataSourceProvider.PARAM_OPERATOR_ID, operatorId)
  .load()


// The schema of stateReadDf follows:
// For streaming aggregation state format v1
// (query ran with lower than Spark 2.4.0 for the first time)
/*
root
 |-- key: struct (nullable = false)
 |    |-- groupKey: integer (nullable = true)
 |-- value: struct (nullable = false)
 |    |-- groupKey: integer (nullable = true)
 |    |-- cnt: long (nullable = true)
 |    |-- sum: long (nullable = true)
 |    |-- max: integer (nullable = true)
 |    |-- min: integer (nullable = true)
*/

// For streaming aggregation state format v2
// (query ran with Spark 2.4.0 or higher for the first time)
/*
root
 |-- key: struct (nullable = false)
 |    |-- groupKey: integer (nullable = true)
 |-- value: struct (nullable = false)
 |    |-- cnt: long (nullable = true)
 |    |-- sum: long (nullable = true)
 |    |-- max: integer (nullable = true)
 |    |-- min: integer (nullable = true)
*/
```

To write Dataset as state of Structured Streaming, you can transform your Dataset as having schema as follows:

```text
root
 |-- key: struct (nullable = false)
 |    |-- ...key fields...
 |-- value: struct (nullable = false)
 |    |-- ...value fields...
```

and add state batch output as follow:

```scala
val operatorId = 0
val batchId = 1 // the version of state for the output of batch is batchId + 1
val newShufflePartitions = 10

df.write
  .format("state")
  .option(StateStoreDataSourceProvider.PARAM_CHECKPOINT_LOCATION,
    new Path(newCheckpointRoot, "state").getAbsolutePath)
  .option(StateStoreDataSourceProvider.PARAM_VERSION, batchId + 1)
  .option(StateStoreDataSourceProvider.PARAM_OPERATOR_ID, operatorId)
  .option(StateStoreDataSourceProvider.PARAM_NEW_PARTITIONS, newShufflePartitions)
  .save() // saveAsTable() also supported
```

Before that, you may want to create a savepoint from existing checkpoint to another path, so that you can simply 
run new Structured Streaming query with modified state.

```scala
// Here we assume 'spark' as SparkSession.
// If you just want to create a savepoint without modifying state, provide `additionalMetadataConf` as `Map.empty`,
// and `excludeState` as `false`.
// That said, if you want to prepare state modification, it would be good to create a savepoint with providing
// addConf to new shuffle partition (like below), and `excludeState` as `true` (to avoid unnecessary copy for state)
val addConf = Map(SQLConf.SHUFFLE_PARTITIONS.key -> newShufflePartitions.toString)
CheckpointUtil.createSavePoint(spark, oldCpPath, newCpPath, newLastBatchId, addConf, excludeState = true)
```

If you ran streaming aggregation query before Spark 2.4.0 and want to upgrade (or already upgraded) to Spark 2.4.0 or higher,
you may also want to migrate your state from state format 1 to 2 (Spark 2.4.0 introduces it) to reduce overall state size,
and get some speedup from most of cases.

Please refer [SPARK_24763](https://issues.apache.org/jira/browse/SPARK-24763) for more details.

```scala
// Here we assume 'spark' as SparkSession.
val stateKeySchema = new StructType()
  .add("groupKey", IntegerType)

val stateValueSchema = new StructType()
  // value schema in state format v1 has columns in key schema
  .add("groupKey", IntegerType)
  .add("cnt", LongType)
  .add("sum", LongType)
  .add("max", IntegerType)
  .add("min", IntegerType)

val stateFormat = new StructType()
  .add("key", stateKeySchema)
  .add("value", stateValueSchema)

val migrator = new StreamingAggregationMigrator(spark)
migrator.convertVersion1To2(oldCpPath, newCpPath, stateKeySchema, stateValueSchema)
```

Please refer the [test codes](https://github.com/HeartSaVioR/spark-state-tool/tree/master/src/test/scala/org/apache/spark/sql/state) to see details on how to use.

## License

Copyright 2019 Jungtaek Lim "<kabhwan@gmail.com>"

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. 
