package spark_demo

import org.apache.spark.sql.SparkSession

object SimpleApp {

  def main(args: Array[String]) {
    val logFile = this.getClass().getResource("dota.txt").toString.substring(6) // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").master("local[1]")
      .config("spark.testing.memory", "5000000000").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("亡灵")).count()
    val numBs = logData.filter(line => line.contains("火女")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs") //Lines with a: 4, Lines with b: 2
    spark.stop()


  }
}
