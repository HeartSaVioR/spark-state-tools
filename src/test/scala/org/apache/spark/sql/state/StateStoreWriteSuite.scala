package org.apache.spark.sql.state

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}
import org.scalatest.{Assertions, BeforeAndAfterAll}

class StateStoreWriteSuite extends StateStoreTest with BeforeAndAfterAll with Assertions {
  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    spark.sql("drop table if exists tbl")
  }

  test("rescale state from streaming aggregation - state format version 1") {
    withSQLConf(Seq(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> "1"): _*) {
      withTempDir { tempDir =>
        runLargeDataStreamingAggregationQuery(tempDir.getAbsolutePath)

        val stateKeySchema = new StructType()
          .add("groupKey", IntegerType)

        val stateValueSchema = new StructType()
          .add("groupKey", IntegerType)
          .add("cnt", LongType)
          .add("sum", LongType)
          .add("max", IntegerType)
          .add("min", IntegerType)

        val stateSchema = new StructType()
          .add("key", stateKeySchema)
          .add("value", stateValueSchema)

        val stateReadDf = spark.read
          // FIXME: why it is not registered by default?
          .format("org.apache.spark.sql.state")
          .schema(stateSchema)
          .option(DefaultSource.PARAM_CHECKPOINT_LOCATION,
            new File(tempDir, "state").getAbsolutePath)
          .option(DefaultSource.PARAM_BATCH_ID, "2")
          .option(DefaultSource.PARAM_OPERATOR_ID, "0")
          .load()

        val expectedRows = stateReadDf.collect()

        stateReadDf.write
          // FIXME: why it is not registered by default?
          .format("org.apache.spark.sql.state")
          .option(DefaultSource.PARAM_CHECKPOINT_LOCATION,
            new File(tempDir, "state2").getAbsolutePath)
          .option(DefaultSource.PARAM_BATCH_ID, "2")
          .option(DefaultSource.PARAM_OPERATOR_ID, "0")
          .option(DefaultSource.PARAM_NEW_PARTITIONS, "20")
          .saveAsTable("tbl")

        // verify write-and-read works
        checkAnswer(spark.sql("select * from tbl"), expectedRows)

        // read again
        val stateReadDf2 = spark.read
          // FIXME: why it is not registered by default?
          .format("org.apache.spark.sql.state")
          .schema(stateSchema)
          .option(DefaultSource.PARAM_CHECKPOINT_LOCATION,
            new File(tempDir, "state2").getAbsolutePath)
          .option(DefaultSource.PARAM_BATCH_ID, "2")
          .option(DefaultSource.PARAM_OPERATOR_ID, "0")
          .load()

        checkAnswer(stateReadDf2, expectedRows)

        // TODO: rerun streaming query with stored state when we can modify metadata
      }
    }
  }

  test("rescale state from streaming aggregation - state format version 2") {
    withSQLConf(Seq(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> "2"): _*) {
      withTempDir { tempDir =>
        runLargeDataStreamingAggregationQuery(tempDir.getAbsolutePath)

        val stateKeySchema = new StructType()
          .add("groupKey", IntegerType)

        val stateValueSchema = new StructType()
          .add("cnt", LongType)
          .add("sum", LongType)
          .add("max", IntegerType)
          .add("min", IntegerType)

        val stateSchema = new StructType()
          .add("key", stateKeySchema)
          .add("value", stateValueSchema)

        val stateReadDf = spark.read
          // FIXME: why it is not registered by default?
          .format("org.apache.spark.sql.state")
          .schema(stateSchema)
          .option(DefaultSource.PARAM_CHECKPOINT_LOCATION,
            new File(tempDir, "state").getAbsolutePath)
          .option(DefaultSource.PARAM_BATCH_ID, "2")
          .option(DefaultSource.PARAM_OPERATOR_ID, "0")
          .load()

        val expectedRows = stateReadDf.collect()

        stateReadDf.write
          // FIXME: why it is not registered by default?
          .format("org.apache.spark.sql.state")
          .option(DefaultSource.PARAM_CHECKPOINT_LOCATION,
            new File(tempDir, "state2").getAbsolutePath)
          .option(DefaultSource.PARAM_BATCH_ID, "2")
          .option(DefaultSource.PARAM_OPERATOR_ID, "0")
          .option(DefaultSource.PARAM_NEW_PARTITIONS, "20")
          .saveAsTable("tbl")

        // verify write-and-read works
        checkAnswer(spark.sql("select * from tbl"), expectedRows)

        // read again
        val stateReadDf2 = spark.read
          // FIXME: why it is not registered by default?
          .format("org.apache.spark.sql.state")
          .schema(stateSchema)
          .option(DefaultSource.PARAM_CHECKPOINT_LOCATION,
            new File(tempDir, "state2").getAbsolutePath)
          .option(DefaultSource.PARAM_BATCH_ID, "2")
          .option(DefaultSource.PARAM_OPERATOR_ID, "0")
          .load()

        checkAnswer(stateReadDf2, expectedRows)

        // TODO: rerun streaming query with stored state when we can modify metadata
      }
    }
  }

  test("simple state schema evolution from streaming aggregation - state format version 2") {
    withSQLConf(Seq(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> "2"): _*) {
      withTempDir { tempDir =>
        runSmallDataStreamingAggregationQuery(tempDir.getAbsolutePath)

        val stateKeySchema = new StructType()
          .add("groupKey", IntegerType)

        val stateValueSchema = new StructType()
          .add("cnt", LongType)
          .add("sum", LongType)
          .add("max", IntegerType)
          .add("min", IntegerType)

        val stateSchema = new StructType()
          .add("key", stateKeySchema)
          .add("value", stateValueSchema)

        val stateReadDf = spark.read
          // FIXME: why it is not registered by default?
          .format("org.apache.spark.sql.state")
          .schema(stateSchema)
          .option(DefaultSource.PARAM_CHECKPOINT_LOCATION,
            new File(tempDir, "state").getAbsolutePath)
          .option(DefaultSource.PARAM_BATCH_ID, "2")
          .option(DefaultSource.PARAM_OPERATOR_ID, "0")
          .load()

        // rows: (1, 2, 6, 3, 3), (0, 1, 2, 2, 2)

        val evolutionDf = stateReadDf
          // without casting to double it goes to decimal
          // avg() returns decimal if input is decimal type, otherwise double
          .selectExpr("key", "value", "CAST((1.0 * value.sum / value.cnt) AS DOUBLE) AS avg")
          .selectExpr("key", "struct(value.*, avg) AS value")

        // new rows: (1, 2, 6, 3, 3, 3.0), (0, 1, 2, 2, 2, 2.0)
        val expectedRows = Seq(
          Row(Row(1), Row(2, 6, 3, 3, 3.0d)),
          Row(Row(0), Row(1, 2, 2, 2, 2.0d)))

        evolutionDf.printSchema()

        evolutionDf.write
          // FIXME: why it is not registered by default?
          .format("org.apache.spark.sql.state")
          .option(DefaultSource.PARAM_CHECKPOINT_LOCATION,
            new File(tempDir, "state2").getAbsolutePath)
          .option(DefaultSource.PARAM_BATCH_ID, "2")
          .option(DefaultSource.PARAM_OPERATOR_ID, "0")
          .option(DefaultSource.PARAM_NEW_PARTITIONS, "20")
          .saveAsTable("tbl")

        // verify write-and-read works
        checkAnswer(spark.sql("select * from tbl"), expectedRows)

        val newStateSchema = new StructType(stateSchema.fields.map { field =>
          if (field.name == "value") {
            // we've casted to double, so adding double type here
            StructField("value", field.dataType.asInstanceOf[StructType].add("avg", DoubleType))
          } else {
            field
          }
        })

        println(newStateSchema)

        // read again
        val stateReadDf2 = spark.read
          // FIXME: why it is not registered by default?
          .format("org.apache.spark.sql.state")
          .schema(newStateSchema)
          .option(DefaultSource.PARAM_CHECKPOINT_LOCATION,
            new File(tempDir, "state2").getAbsolutePath)
          .option(DefaultSource.PARAM_BATCH_ID, "2")
          .option(DefaultSource.PARAM_OPERATOR_ID, "0")
          .load()

        checkAnswer(stateReadDf2, expectedRows)

        // TODO: rerun streaming query with stored state when we can modify metadata
      }
    }
  }
}
