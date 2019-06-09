package spark_demo

import org.apache.spark.sql.SparkSession

object SimpleApp {

  def main(args: Array[String]) {
    sessionDemo2


  }

  private def sessionDemo1 = {
    val logFile = this.getClass().getResource("dota.txt").toString.substring(6) // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").master("local[1]")
      .config("spark.testing.memory", "5000000000").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("亡灵")).count()
    val numBs = logData.filter(line => line.contains("火女")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs") //Lines with a: 4, Lines with b: 2
    spark.stop()
  }

  private def sessionDemo2 = {
    val logFile = this.getClass().getResource("json.json").toString.substring(6) // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").master("local[1]")
      .config("spark.testing.memory", "5000000000")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val df = spark.read.format("json").load(logFile)
    df.printSchema()
//    df.show()
  }
}
