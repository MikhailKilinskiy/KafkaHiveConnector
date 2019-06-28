package com.bigdata.testing.afosp

import com.typesafe.config.{Config, ConfigObject}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.spark.SparkConf
import java.util.Properties

import collection.JavaConverters._
import scala.collection.mutable

object Configuration {

  def batchDuration(config: Config): Long = config.getDuration("batch.duration").toMillis

  def sparkConf(config: Config): SparkConf = {
    val name = config.getConfig("spark").getString("app.name")
    val cfg = new SparkConf()
      .setMaster("yarn")
      .setAppName(name)

    cfg
  }

  def schemaRegistryUrl(config: Config): String = config.getConfig("schema").getString("url")

  def schemaFromSubject(config: Config): String = config.getConfig("schema").getString("from.subject")

  def schemaToSubject(config: Config): String = config.getConfig("schema").getString("to.subject")

  def hiveFromTable(config: Config): String = config.getConfig("hive").getString("from.table")

  def hiveToTable(config: Config): String = config.getConfig("hive").getString("to.table")

  def saveMode(config: Config): String = config.getConfig("hive").getString("save.mode")

  def bootstrapServers(config: Config): String = config.getConfig("kafka").getString("bootstrap.servers")

  def securityProtocol(config: Config): String = config.getConfig("kafka").getString("security.protocol")

  def remoteHdfs(config: Config): String = config.getConfig("hdfs").getString("remote.path")

  def remoteNameNode(config: Config): String = config.getConfig("hdfs").getString("remote.namenode")

  def consumerProps(config: Config): Map[String, Object] = {
    val kafkaConf =  config.getConfig("kafka")
    val props = new Properties()

    mutable.Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaConf.getString("bootstrap.servers"),
      ConsumerConfig.GROUP_ID_CONFIG -> kafkaConf.getString("group.id"),
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer],
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> kafkaConf.getString("reader.strategy"),
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> kafkaConf.getString("enable.auto.commit.offset"),
      "security.protocol" -> kafkaConf.getString("security.protocol"),
      "sasl.mechanism" -> kafkaConf.getString("sasl.mechanism")
    ).toMap

  }

  def toTopic(config: Config): String = config.getConfig("kafka").getString("to.topic.name")

  def fromTopic(config: Config): String = config.getConfig("kafka").getString("from.topic.name")

}
