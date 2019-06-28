package com.bigdata.testing.afosp

object RunJob {

  val usage = """
    Usage: parametr [--job] must be Hive2Kafka, Kafka2Hive, Kafka2RemoteHDFS
  """

  def main(args: Array[String]): Unit = {

    if (args.length == 0) println(usage)
    val arglist = args.toList
    if(arglist(0) != "--job") {
      println(usage)
    } else {
      arglist(1) match {
        case "Hive2Kafka" => Strategies.hive2kafka()
        case "Kafka2Hive" => Strategies.kafka2hive()
        case "Kafka2RemoteHDFS" => Strategies.kafka2hdfs()
        case "Kafka2HDFS" => Strategies.kafka2localhdfs()
        case _ => println(usage)
      }
    }
  }

}
