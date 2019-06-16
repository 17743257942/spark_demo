package spark_sql

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

object SparkSql {
  def main(args: Array[String]): Unit = {
    //    val logFile = "E:\\Program Files\\vm_linux\\share\\aa.txt"
    val logFile = this.getClass().getResource("log.txt").toString.substring(6)
    val conf = new SparkConf()
    conf.setMaster("local[1]")
    conf.setAppName("test0")
    conf.set("spark.testing.memory", "5000000000")
    val sc = new SparkContext(conf)
    println("create my first SparkContext ----------")

    val rdd = sc.textFile(logFile)

    println(rdd.collect().mkString("\n"))

    // 将结果进行map处理
    //    rdd.map(line => {
    //      val splits = line.split(" ")
    //      val ip = splits(0)
    //      // [10/Nov/2016:00:01:02 +0800] ==> yyyy-MM-dd HH:mm:ss
    //      val time = splits(3) + " " + splits(4)
    //      val url = splits(8)
    //      val nums = splits(9)
    //      (ip, DateUtils.parse(time), url, nums)
    //    }).take(10).foreach(println)

    // 将结果写到文件里
    val toFile = "E:\\Program Files\\vm_linux\\share\\spark_sql_out"
    // 如果文件夹存在就删除
    FileUtils.dirDel(new File(toFile))

    rdd.map(line => {
      val splits = line.split(" ")
      val ip = splits(0)
      // [10/Nov/2016:00:01:02 +0800] ==> yyyy-MM-dd HH:mm:ss
      val time = splits(3) + " " + splits(4)
      val url = splits(8)
      val nums = splits(9)
      DateUtils.parse(time) + "\002" + url + "\002" + nums + "\002" + ip
    }).saveAsTextFile(toFile)


    sc.stop()
  }


}
