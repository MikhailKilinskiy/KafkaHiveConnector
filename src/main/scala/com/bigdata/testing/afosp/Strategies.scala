package com.bigdata.testing.afosp

import com.bigdata.testing.afosp.jobs.{KafkaReader, KafkaWriter}
import com.bigdata.testing.afosp.schema.SchemaRegistryUtil
import com.bigdata.testing.afosp.serde.{SparkAvroDeserializer, SparkAvroSerializer}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.bigdata.testing.afosp.jobs.{KafkaReader, KafkaWriter}
import com.bigdata.testing.afosp.serde.{SparkAvroDeserializer, SparkAvroSerializer}

object Strategies {

  val jobConfig: Config = ConfigFactory.load().getConfig("org")

  def fromKafka(remote: Boolean): KafkaReader = {

    val schemaRegistry = new SchemaRegistryUtil(jobConfig)

    val (schemaId, avroSchema) = schemaRegistry.getSchemaAndId(Configuration.schemaFromSubject(jobConfig))

    val deserializer: SparkAvroDeserializer = SparkAvroDeserializer(schemaId, avroSchema)

    val kafkaReader: KafkaReader = KafkaReader(deserializer, jobConfig, remote)

    kafkaReader
  }


  def hive2kafka(): Unit = {

    val spark = SparkSession
      .builder()
      .appName("hive2kafka")
      .master("yarn")
      .enableHiveSupport()
      .getOrCreate()

    val schemaRegistry = new SchemaRegistryUtil(jobConfig)

    val (schemaId, avroSchema) = schemaRegistry.getSchemaAndId(Configuration.schemaToSubject(jobConfig))

    val df: DataFrame = spark.table(Configuration.hiveFromTable(jobConfig))

    val serializer: SparkAvroSerializer = SparkAvroSerializer(schemaId, avroSchema, df)

    val kafkaWriter: KafkaWriter = KafkaWriter(serializer, jobConfig, df)

    kafkaWriter.start(spark)
  }


  def kafka2hive(): Unit = {

    val spark = SparkSession
      .builder()
      .appName("kafka2hive")
      .master("yarn")
      .enableHiveSupport()
      .getOrCreate()

    val kafkaReader: KafkaReader = fromKafka(false)

    kafkaReader.start(spark)
  }

  def kafka2hdfs(): Unit = {

    val spark = SparkSession
      .builder()
      .appName("kafka2hdfs")
      .master("yarn")
      .enableHiveSupport()
      .config("spark.yarn.access.hadoopFileSystems", Configuration.remoteNameNode(jobConfig))
      .config("spark.hadoop.ipc.client.fallback-to-simple-auth-allowed", "true")
      .getOrCreate()

    val kafkaReader: KafkaReader = fromKafka(true)

    kafkaReader.start(spark)

  }

  def kafka2localhdfs(): Unit = {

    val spark = SparkSession
      .builder()
      .appName("kafka2localhdfs")
      .master("yarn")
      .enableHiveSupport()
      .getOrCreate()

    val kafkaReader: KafkaReader = fromKafka(true)

    kafkaReader.start(spark)

  }

}
