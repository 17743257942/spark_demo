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
    val lines = rdd.flatMap(line => line.split(" ")) // 压扁文档，将所有行汇聚
    val count1 = lines.map(word => (word, 1)) // 使用map算子将元素变成键值对
    println(count1.collect().mkString(","))
    val count2 = count1.reduceByKey { case (x, y) => x + y } //case可以去掉
//    val count2 = count1.reduceByKey { _ + _ } //不同RDD中相同key值拉到一起进行value的归并操作
    println(count2.collect().mkString(","))
    val count3 = count2.map(x => (x._2, x._1)) // k v 调转顺序 变成<v,k>
    println(count3.collect().mkString(","))
    val count4 = count3.sortByKey(false) // 按 v 降序排列
    println(count4.collect().mkString(","))
    val count5 = count4.map(x => (x._2, x._1)) // 再调转顺序，恢复原样<k,v>
    println(count5.collect().mkString(","))
    //    count.saveAsTextFile("E:\\Program Files\\vm_linux\\share\\aaout2.txt") //保存到一个不存在的目录
    println()
    println()
    println()

    val rdd2 = rdd.flatMap(_.split(" "))
    println(rdd2.collect().mkString(","))  //hello,world,hello,my,world,hello,world,of,mine,hello,worlds
    val rdd3 = rdd.map(_.split(" "))
    println(rdd3.first()) // [Ljava.lang.String;@3f3c96
    println(rdd3.count()) // 4
    /* 使用makeRDD创建RDD */
    /* List */
    val rdd01 = sc.makeRDD(List(1, 2, 3, 4, 5, 6))
    println(rdd01.count()) //6
    println(rdd01.min()) //1
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
