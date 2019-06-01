package spark_demo

import org.apache.spark.{SparkConf, SparkContext}


object Demo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("test0")
    conf.set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(1, 2, 3, 4))
    val e = rdd.first()
    rdd.foreach(p => {
      val tname = Thread.currentThread().getName
      println(tname + "\t" + p)
    })
    println(e)


  }
}
