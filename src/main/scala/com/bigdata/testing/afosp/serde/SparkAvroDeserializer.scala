package com.bigdata.testing.afosp.serde

import org.apache.avro.io.DecoderFactory
import java.io.ByteArrayOutputStream

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.EncoderFactory
import org.apache.spark.SparkException
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.UserDefinedFunction

import scala.util.{Failure, Success, Try}


class SparkAvroDeserializer(id: Int, schema_string: String) extends AbstractSparkAvroSerDe {

  def deserializeToJson(payload: Array[Byte]): Option[String] = Try {

    val schema = new Schema.Parser().parse(schema_string)

    if(payload(0) == MAGIC_BYTE) {
      val bytes = payload.slice(HEADER_SIZE, payload.size)
      val binaryDecoder = DecoderFactory.get.binaryDecoder(bytes, null)
      val record = new GenericDatumReader[GenericRecord](schema).read(null, binaryDecoder)
      val outputStream = new ByteArrayOutputStream()
      val jsonEncoder = EncoderFactory.get.jsonEncoder(schema, outputStream, false)
      val writer = new GenericDatumWriter[GenericRecord](schema)
      writer.write(record, jsonEncoder)
      jsonEncoder.flush
      val result = outputStream.toByteArray

      new String(result, "UTF-8")

    } else {
      throw new SparkException("Magic byte must be 0, not "+payload(0))
    }
  } match {
    case Success (value) => Some(value)
    case Failure (exception) => throw new SparkException("Error while deserializing to Json ", exception)
  }

  val deserializeToJsonUDF: UserDefinedFunction = udf(deserializeToJson _)
}

object SparkAvroDeserializer {
  def apply(id: Int, schema_string: String): SparkAvroDeserializer = new SparkAvroDeserializer(id, schema_string)
}
