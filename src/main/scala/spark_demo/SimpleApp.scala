package spark_demo

import org.apache.spark.sql.SparkSession
object SimpleApp {

  def main(args: Array[String]) {
//    sessionDemo1
//    println("===============================================================")
//    sessionDemo2
//    println("===============================================================")
//    sessionDemo3
//    println("===============================================================")
    sessionDemo4
//    println("===============================================================")

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
    val df1 = df.withColumnRenamed("_1", "name")
      .withColumnRenamed("_2", "age")
      .withColumnRenamed("_3", "phone")
      .withColumnRenamed("_4", "qwer") // 没有这列，所以不显示
    df1.show()
    val df2 = df1.withColumnRenamed("name", "asdf")
    df2.show()

    df1.createOrReplaceTempView("people") //对上面的dataframe创建一个表
    df1.cache()
    //缓存表
    val resultsDF = spark.sql("SELECT name, age, phone FROM people where age<20") //对表调用SQL语句
    resultsDF.show(10) //展示结果

  }

  private def sessionDemo3 = {
    val logFile = this.getClass().getResource("json.json").toString.substring(6) // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application1").master("local[1]")
      .config("spark.testing.memory", "5000000000").getOrCreate()


    val df0 = spark.read.format("json").load(logFile)
    df0.show()
    df0.select("name","Education").show()
    df0.createOrReplaceTempView("json")
    df0.cache()
    spark.sql("desc   json ").show()
    spark.sql("select Education from   json ").show() //对表调用SQL语句



  }

  import org.apache.spark._

  private def sessionDemo4 = {
    val logFile = this.getClass().getResource("json.json").toString.substring(6) // Should be some file on your system
    val conf = new SparkConf()
    conf.setMaster("local[1]")
    conf.setAppName("test0")
    conf.set("spark.testing.memory", "5000000000")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(logFile)
    rdd.take(1).foreach(println)
    println(rdd.collect().mkString(","))




  }
}
