package com.bigdata.testing.afosp.jobs

import com.bigdata.testing.afosp.{Configuration, LazyLogging}
import com.bigdata.testing.afosp.serde.SparkAvroDeserializer
import com.bigdata.testing.afosp.transformers.FromJsonTransformer

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import com.typesafe.config.Config
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import com.bigdata.testing.afosp.serde.{SparkAvroDeserializer, SparkAvroSerializer}
import com.bigdata.testing.afosp.{Configuration, LazyLogging}

class KafkaReader (
                    deserializer: SparkAvroDeserializer,
                    config: Config,
                    remote: Boolean
                  ) extends Job with LazyLogging {

  override def start(spark: SparkSession): Unit = {

    val ssc = new StreamingContext(spark.sparkContext, batchDuration = Seconds(3))

    val stream = KafkaUtils.createDirectStream(
      ssc,
      PreferConsistent,
      Subscribe[Array[Byte], Array[Byte]](
        Configuration.fromTopic(config).split(",").toSet,
        Configuration.consumerProps(config)
      )
    )

    val saveMode = Configuration.saveMode(config)

    val rowStream =  stream.map { record => (
      record.key(),
      record.value(),
      record.topic(),
      record.partition(),
      record.offset(),
      record.timestamp()
    )
    }

    def dropTable(): Unit = {
      if(saveMode=="overwrite") {
        spark.sql("DROP TABLE IF EXISTS " + Configuration.hiveToTable(config))
        logger.info("Hive table was truncated")
      } else {
        logger.info("Hive table was NOT truncated")
      }
    }

    if(!remote) dropTable()

    rowStream
      .foreachRDD(rdd => {
        if (!rdd.isEmpty()) {
          import spark.implicits._
          import spark.sqlContext.implicits._
          val batchDF = spark.createDataFrame(rdd).toDF("key","value","topic","partition","offset","timestamp")
          val jsonDF = batchDF.withColumn("json_value", deserializer.deserializeToJsonUDF(col("value")))
          val transformedDF = FromJsonTransformer.transform(jsonDF, spark)


          if(!remote) {
            logger.info("Write batch to Hive table")
            transformedDF.write.mode("append").saveAsTable(Configuration.hiveToTable(config))
          } else {
            logger.info("Write batch to HDFS path")
            transformedDF.write.mode("append").orc(Configuration.remoteHdfs(config))
          }
        }
      }
      )

    ssc.start()
    ssc.awaitTermination()

  }
}

object KafkaReader {
  def apply(
             deserializer: SparkAvroDeserializer,
             config: Config,
             remote: Boolean
           ): KafkaReader = new KafkaReader(deserializer, config, remote)
}
