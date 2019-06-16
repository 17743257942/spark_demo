package spark_sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlETL {
  def main(args: Array[String]): Unit = {

    val logFile = "E:\\Program Files\\vm_linux\\share\\spark_sql_out\\part-00000" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").master("local[1]")
      .config("spark.testing.memory", "5000000000").getOrCreate()
    // rdd的作用是装载数据
    val rdd = spark.sparkContext.textFile(logFile)
    println("原数据：\n" + rdd.collect().mkString("\n"))
    // rdd ==> df
    val df = spark.createDataFrame(rdd.map(x => ConvertUtil.parseLog(x)), ConvertUtil.struct)

    df.printSchema()
    df.show()


    //    df.createOrReplaceTempView("log")
    //    spark.sql("select * from   log ").show()

    spark.stop()
  }
}
