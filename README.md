# Spark State Tools

Spark State Tools provides features about offline manipulation of Structured Streaming state on existing query.

The features we provide as of now are:

* Read state as batch source of Spark SQL
* Write DataFrame to state as batch sink of Spark SQL
  * Repartitioning is supported while writing state
* Create savepoint from existing checkpoint of Structured Streaming query
  * You can pick specific batch (if it exists on metadata) to create savepoint.
  * With feature of writing state, you can achieve rescaling state, simple schema evolution, etc.

As this project leverages Spark Structured Streaming's interfaces, and doesn't deal with internal
(e.g. the structure of state file for HDFS state store), the performance may be suboptimal.

## Disclaimer

This is more of a proof of concept implementation, might not something for production ready.
When you deal with writing state, you may want to backup your checkpoint with CheckpointUtil and try it with backup.

The project is intended to deal with offline state, not against state which streaming query is running.
Actually it can be possible, but state store provider in running query can purge old batches, which would produce error on here.

## How to use

FIXME: Adding artifact to your project will be described here when the artifact is published to Maven central.

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
// If you just want to create a savepoint without modifying state, provide `newShufflePartitions` as `None`,
// and `excludeState` as `false`.
// That said, if you want to prepare state modification, it would be good to create a savepoint with providing
// `newShufflePartitions` as `Some(newValue)`, and `excludeState` as `true` (to avoid unnecessary copy for state)
CheckpointUtil.createSavePoint(spark, oldCpPath, newCpPath, newLastBatchId,
  newShufflePartitions = Some(newShufflePartitions),
  excludeState = true)
```

Please refer the test codes to see details on how to use.

## TODO

* Release and publish to Maven central
* Sort out regarding license things: Copyright, License regarding dependencies, etc.
* More documentation
* Move to Maven (SBT is just used as quick start and I don't like that much)
* Apply Scala Checkstyle
* Optimization against HDFS state store (if we got some requests)

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
