package com.bigdata.testing.afosp.serde

import org.apache.spark.sql.Row


trait AbstractSparkAvroSerDe extends Serializable {

  protected val MAGIC_BYTE = 0x0
  protected val idSize = 4
  protected val HEADER_SIZE = 5


}
