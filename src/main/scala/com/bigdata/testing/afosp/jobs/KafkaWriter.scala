package com.bigdata.testing.afosp.jobs

import com.bigdata.testing.afosp.Configuration
import com.bigdata.testing.afosp.serde.SparkAvroSerializer
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import com.typesafe.config.Config
import com.bigdata.testing.afosp.serde.SparkAvroSerializer
import com.bigdata.testing.afosp.Configuration

class KafkaWriter (
                    serializer: SparkAvroSerializer,
                    config: Config,
                    rawDF: DataFrame
                  ) extends Job {

  override def start(spark: SparkSession): Unit = {

    val avroDF = rawDF.select(
      serializer.serializeToAvroUDF(struct(rawDF.columns.map(col):_*)).alias("value")
    )

    avroDF
      .selectExpr("uuid() as key", "value as value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", Configuration.bootstrapServers(config))
      .option("kafka.security.protocol", Configuration.securityProtocol(config))
      .option("topic", Configuration.toTopic(config))
      .save()
  }

}

object KafkaWriter {
  def apply(
             serializer: SparkAvroSerializer,
             config: Config,
             rawDF: DataFrame
           ): KafkaWriter = new KafkaWriter(serializer, config, rawDF)
}
