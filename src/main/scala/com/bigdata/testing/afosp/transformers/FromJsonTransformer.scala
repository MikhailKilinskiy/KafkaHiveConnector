package com.bigdata.testing.afosp.transformers

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.util.{Failure, Success, Try}
import org.apache.spark.SparkException

object FromJsonTransformer extends Transformer {

  private var cashedSchema: StructType = null

  def jsonStrToMap(jsonStr: String): Map[String, Map[String, Any]] = {
    implicit val formats = org.json4s.DefaultFormats

    parse(jsonStr).extract[Map[String, Map[String, Any]]]
  }

  override def transform(df: DataFrame, spark: SparkSession): DataFrame = Try {

    if(cashedSchema == null) {
      cashedSchema = spark.read.json(df.rdd.map(r => r.getAs[String]("json_value"))).schema
    }

    df.withColumn("metrics", from_json(col("json_value"), cashedSchema)).createOrReplaceTempView("tmp_metrics")

    val query_str = df.first().getAs[String]("json_value")
    val schemaMap = jsonStrToMap(query_str)
    var select = "select key, CAST(key as String) as keyString, topic, partition, offset, timestamp, json_value, "

    schemaMap.foreach{case (name, tp) => select += s"metrics.$name.${tp.keys.head} as $name, "}

    val queryString = select.substring(0, select.length - 2) + " from tmp_metrics"

    spark.sql(queryString)
  } match {
    case Success(value) => value
    case Failure(exception) => throw new SparkException("Error while transforming dataframe", exception)
  }

}
