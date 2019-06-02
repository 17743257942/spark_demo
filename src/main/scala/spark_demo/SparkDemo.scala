package spark_demo

import org.apache.spark.{SparkConf, SparkContext}

object SparkDemo {
  def main(args: Array[String]): Unit = {
    val logFile = "E:\\Program Files\\vm_linux\\share\\aa.txt"
    val conf = new SparkConf()
    conf.setMaster("local[1]")
    conf.setAppName("test0")
    conf.set("spark.testing.memory", "5000000000")
    val sc = new SparkContext(conf)
    println("create my first SparkContext ----------")

    val rdd = sc.textFile(logFile)
    /**
      * 第一种wc
      */
    //    val wordcount = rdd.flatMap(_.split(" "))
    //      .map((_, 1)).reduceByKey(_ + _)
    //      .map(x => (x._2, x._1))
    //      .sortByKey(false)
    //      .map(x => (x._2, x._1))
    //    wordcount.saveAsTextFile("E:\\Program Files\\vm_linux\\share\\aaout.txt")
    //    sc.stop()
    /**
      * 第二种wc
      */
    val lines = rdd.flatMap(line=>line.split(" "))
    val count = lines.map(word => (word,1)).reduceByKey{case (x,y)=>x+y}
    count.saveAsTextFile("E:\\Program Files\\vm_linux\\share\\aaout2.txt")


    val rdd2 = rdd.flatMap(_.split(" "))
    println(rdd2.collect().mkString(","))
    val rdd3=rdd.map(_.split(" "))
    println(rdd3.first())
    println(rdd3.count())
    /* 使用makeRDD创建RDD */
    /* List */
    val rdd01 = sc.makeRDD(List(1, 2, 3, 4, 5, 6))
    println(rdd01.count())
    println(rdd01.min())
    val r01 = rdd01.map { x => x * x }
    println(r01.collect().mkString(","))
    /* Array */
    val rdd02 = sc.makeRDD(Array(1, 2, 3, 4, 5, 6))
    val r02 = rdd02.filter { x => x < 5 }
    println(r02.collect().mkString(","))

    val rdd03 = sc.parallelize(List(1, 2, 3, 4, 5, 6), 1)
    val r03 = rdd03.map { x => x + 1 }
    println(r03.collect().mkString(","))
    /* Array */
    val rdd04 = sc.parallelize(List(1, 2, 3, 4, 5, 6), 1)
    val r04 = rdd04.filter { x => x > 3 }
    println(r04.collect().mkString(","))


  }


}
