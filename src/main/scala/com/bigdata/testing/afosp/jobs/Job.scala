package com.bigdata.testing.afosp.jobs

import org.apache.spark.sql.SparkSession

trait Job extends Serializable {

  def start(spark: SparkSession): Unit

}
