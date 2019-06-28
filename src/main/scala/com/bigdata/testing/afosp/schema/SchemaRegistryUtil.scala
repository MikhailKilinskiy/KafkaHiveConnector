package com.bigdata.testing.afosp.schema

import com.bigdata.testing.afosp.{Configuration, LazyLogging}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import com.bigdata.testing.afosp.Configuration
import com.typesafe.config.Config

class SchemaRegistryUtil (config: Config) extends Serializable with LazyLogging {

  val schemaRegistryURLs: String = Configuration.schemaRegistryUrl(config)

  @transient private lazy val schemaRegistry = {

    import scala.collection.JavaConverters._
    val urls = schemaRegistryURLs.split(",").toList.asJava
    val cacheCapacity = 128

    new CachedSchemaRegistryClient(urls, cacheCapacity)
  }

  def getSchemaAndId(subject: String): (Int, String) = {
    logger.info(s"Try to retrive schema from: $schemaRegistryURLs.")
    val schemaId = schemaRegistry.getLatestSchemaMetadata(subject).getId
    val avroSchema = schemaRegistry.getByID(schemaId).toString

    (schemaId, avroSchema)
  }
}
