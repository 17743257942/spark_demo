package spark_demo

import org.apache.spark.sql.SparkSession

object SimpleApp {

  def main(args: Array[String]) {
    //    sessionDemo1
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
    val spark = SparkSession.builder().appName("Simple Application")
      .master("local[1]")
      .config("spark.testing.memory", "5000000000")
      //      .enableHiveSupport()
      //      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    /**
      * +----+---+-----------+
      * |name|age|      phone|
      * +----+---+-----------+
      * |ming| 20|15552211521|
      * |hong| 19|13287994007|
      * | zhi| 21|15552211523|
      * +----+---+-----------+
      */
    val df = spark.createDataFrame(Seq(
      ("ming", 20, 15552211521L),
      ("hong", 19, 13287994007L),
      ("zhi", 21, 15552211523L)
      ))
//    toDF("name", "age", "phone")
    val df1 = df.withColumnRenamed("_1","name")
      .withColumnRenamed("_2","age")
      .withColumnRenamed("_3","phone")
      .withColumnRenamed("_4","qwer") // 没有这列，所以不显示
    df1.show()
    val df2 = df1.withColumnRenamed("name","asdf")
    //    val df = spark.read.format("json").load(logFile)
    df2.show()






  }
}
