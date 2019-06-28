package com.bigdata.testing.afosp.transformers

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Transformer extends Serializable {

  def transform(df: DataFrame, spark: SparkSession): DataFrame

}
