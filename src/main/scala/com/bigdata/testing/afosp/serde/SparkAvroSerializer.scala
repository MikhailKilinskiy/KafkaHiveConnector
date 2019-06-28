package com.bigdata.testing.afosp.serde

import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificDatumWriter
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.EncoderFactory
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Row, SparkSession, Column}
import org.apache.avro.Schema
import org.apache.spark.SparkException

import scala.util.{Failure, Success, Try}

class SparkAvroSerializer (id: Int, schema_string: String, df: DataFrame) extends AbstractSparkAvroSerDe {

  val fieldTypes: Map[String, String] = df.dtypes.toMap

  def getDefaultValue(rowName: String): Any = {
    fieldTypes(rowName) match {
      case "IntegerType" | "LongType" => null
      case "StringType" => ""
    }
  }

  def serializeToAvro(row: Row): Option[Array[Byte]] = Try {

    val schema = new Schema.Parser().parse(schema_string)

    val genericRow: GenericRecord = new GenericData.Record(schema)
    row.schema.fieldNames.zipWithIndex.foreach(name =>
      if (!row.isNullAt(name._2)) {
        genericRow.put(name._1, row.getAs(name._2))
      } else {
        genericRow.put(name._1, getDefaultValue(name._1))
      }
    )

    val writer = new SpecificDatumWriter[GenericRecord](schema)
    val out = new ByteArrayOutputStream()
    out.write(MAGIC_BYTE)
    out.write(ByteBuffer.allocate(idSize).putInt(id).array())
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(genericRow, encoder)
    encoder.flush()
    out.close()

    out.toByteArray()
  } match {
    case Success (value) => Some(value)
    case Failure (exception) => throw new SparkException("Error while serializing to Avro ", exception)
  }


val serializeToAvroUDF: UserDefinedFunction = udf((row: Row) => serializeToAvro(row))
}

object SparkAvroSerializer {
  def apply(id: Int, schema_string: String, df: DataFrame): SparkAvroSerializer = new SparkAvroSerializer(id, schema_string, df)
}
