package com.bigdata.testing.afosp

import org.apache.log4j.Logger

trait LazyLogging extends Serializable {
  @transient
  final protected lazy val logger: Logger = Logger.getLogger(getClass)
}
