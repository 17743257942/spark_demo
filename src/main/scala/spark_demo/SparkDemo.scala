package spark_demo

import org.apache.spark.rdd.RDD
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
    println(rdd.collect().mkString(","))
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


    val rdd22 = rdd.flatMap(_.split(" "))
    println(rdd22.collect().mkString(","))  //hello,world,hello,my,world,hello,world,of,mine,hello,worlds
    val rdd3 = rdd.map(_.split(" "))
    println(rdd3.first()) // [Ljava.lang.String;@3f3c96
    println(rdd3.count()) // 4
    /* 使用makeRDD创建RDD */
    /* List */
    val rdd011 = sc.makeRDD(List(1, 2, 3, 4, 5, 6))
    println(rdd011.count()) //6
    println(rdd011.min()) //1
    val r01 = rdd011.map { x => x * x }
    println(r01.collect().mkString(","))
    /* Array */
    val rdd022 = sc.makeRDD(Array(1, 2, 3, 4, 5, 6))
    val r02 = rdd022.filter { x => x < 5 }
    println(r02.collect().mkString(","))

    val rdd03 = sc.parallelize(List(1, 2, 3, 4, 5, 6), 1)
    val r03 = rdd03.map { x => x + 1 }
    println(r03.collect().mkString(","))
    /* Array */
    val rdd04 = sc.parallelize(List(1, 2, 3, 4, 5, 6), 1)
    val r04 = rdd04.filter { x => x > 3 }
    println(r04.collect().mkString(","))




    val rddInt:RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,2,5,1))
    val rddStr:RDD[String] = sc.parallelize(Array("a","b","c","d","b","a"), 1)
    val rddFile2:RDD[String] = sc.textFile(logFile, 1)

    val rdd01:RDD[Int] = sc.makeRDD(List(1,3,5,3))
    val rdd02:RDD[Int] = sc.makeRDD(List(2,4,5,1))
    /* map操作 */
    println("======map操作======")
    println(rddInt.map(x => x + 1).collect().mkString(","))
    /* filter操作 */
    println("======filter操作======")
    println(rddInt.filter(x => x > 4).collect().mkString(","))
    /* flatMap操作 */
    println("======flatMap操作======")
    println(rddFile2.flatMap { x => x.split(" ") }.first())
    /* distinct去重操作 */
    println("======distinct去重======")
    println(rddInt.distinct().collect().mkString(","))
    println(rddStr.distinct().collect().mkString(","))
    /* union操作 */
    println("======union操作======")
    println(rdd01.union(rdd02).collect().mkString(","))
    /* intersection操作 */
    println("======intersection操作======")
    println(rdd01.intersection(rdd02).collect().mkString(","))
    /* subtract操作 */
    println("======subtract操作======")
    println(rdd01.subtract(rdd02).collect().mkString(","))
    /* cartesian操作 */
    println("======cartesian操作======")
    println(rdd01.cartesian(rdd02).collect().mkString(","))


      // Pair RDD及键值对RDD，Spark里创建Pair RDD也是可以通过两种途径，一种是从内存里读取，一种是从文件读取。
    /**
      * reduceByKey：合并具有相同键的值；
      * groupByKey：对具有相同键的值进行分组；
      * keys：返回一个仅包含键值的RDD；
      * values：返回一个仅包含值的RDD；
      * sortByKey：返回一个根据键值排序的RDD；
      * flatMapValues：针对Pair RDD中的每个值应用一个返回迭代器的函数，然后对返回的每个元素都生成一个对应原键的键值对记录；
      * mapValues：对Pair RDD里每一个值应用一个函数，但是不会对键值进行操作；
      * combineByKey：使用不同的返回类型合并具有相同键的值；
      * subtractByKey：操作的RDD我们命名为RDD1，参数RDD命名为参数RDD，剔除掉RDD1里和参数RDD中键相同的元素；
      * join：对两个RDD进行内连接；
      * rightOuterJoin：对两个RDD进行连接操作，第一个RDD的键必须存在，第二个RDD的键不再第一个RDD里面有那么就会被剔除掉，相同键的值会被合并；
      * leftOuterJoin：对两个RDD进行连接操作，第二个RDD的键必须存在，第一个RDD的键不再第二个RDD里面有那么就会被剔除掉，相同键的值会被合并；
      * cogroup：将两个RDD里相同键的数据分组在一起
      * countByKey：对每个键的元素进行分别计数；
      * collectAsMap：将结果变成一个map；
      * lookup：在RDD里使用键值查找数据
      * take(num):返回RDD里num个元素，随机的；
      * top(num):返回RDD里最前面的num个元素，这个方法实用性还比较高；
      * takeSample：从RDD里返回任意一些元素；
      * sample：对RDD里的数据采样；
      * takeOrdered：从RDD里按照提供的顺序返回最前面的num个元素
      */
    val sparkdata01=  this.getClass().getResource("sparkdata01.txt").toString.substring(6)
    val rddFile:RDD[(String,String)] = sc.textFile(sparkdata01, 1)
      .map { x => (x.split(",")(0),x.split(",")(1) + "," + x.split(",")(2)) }
    println(rddFile.collect().mkString(","))
    val rFile:RDD[String] = rddFile.keys
    val rFile2:RDD[String] = rddFile.values
    println("=========createPairMap File=========")
    println(rFile.collect().mkString(","))// x01,x02,x01,x01,x02,x03
    println(rFile2.collect().mkString("  "))//1,4  11,1  3,9  2,6  18,12  7,9

    val rdd2:RDD[(String,Int)] = sc.makeRDD(List(("k01",3),("k02",6),("k03",2),("k01",26)))
    val r:RDD[(String,Int)] = rdd2.reduceByKey((x,y) => x + y)
    println("=========createPairMap=========")
    println(r.collect().mkString(","))// (k01,29),(k03,2),(k02,6)

    val other:RDD[(String,Int)] = sc.parallelize(List(("k01",29)), 1)

    // 转化操作
    val rddReduce:RDD[(String,Int)] = rdd2.reduceByKey((x,y) => x + y)
    println("====reduceByKey===:" + rddReduce.collect().mkString(","))// (k01,29),(k03,2),(k02,6)
    val rddGroup:RDD[(String,Iterable[Int])] = rdd2.groupByKey()
    println("====groupByKey===:" + rddGroup.collect().mkString(","))// (k01,CompactBuffer(3, 26)),(k03,CompactBuffer(2)),(k02,CompactBuffer(6))
    val rddKeys:RDD[String] = rdd2.keys
    println("====keys=====:" + rddKeys.collect().mkString(","))// k01,k02,k03,k01
    val rddVals:RDD[Int] = rdd2.values
    println("======values===:" + rddVals.collect().mkString(","))// 3,6,2,26
    val rddSortAsc:RDD[(String,Int)] = rdd2.sortByKey(true, 1)
    val rddSortDes:RDD[(String,Int)] = rdd2.sortByKey(false, 1)
    println("====rddSortAsc=====:" + rddSortAsc.collect().mkString(","))// (k01,3),(k01,26),(k02,6),(k03,2)
    println("======rddSortDes=====:" + rddSortDes.collect().mkString(","))// (k03,2),(k02,6),(k01,3),(k01,26)
    val rddFmVal:RDD[(String,Int)] = rdd2.flatMapValues { x => List(x + 10) }
    println("====flatMapValues===:" + rddFmVal.collect().mkString(","))// (k01,13),(k02,16),(k03,12),(k01,36)
    val rddMapVal:RDD[(String,Int)] = rdd2.mapValues { x => x + 10 }
    println("====mapValues====:" + rddMapVal.collect().mkString(","))// (k01,13),(k02,16),(k03,12),(k01,36)
    val rddCombine:RDD[(String,(Int,Int))] = rdd2.combineByKey(x => (x,1), (param:(Int,Int),x) => (param._1 + x,param._2 + 1), (p1:(Int,Int),p2:(Int,Int)) => (p1._1 + p2._1,p1._2 + p2._2))
    println("====combineByKey====:" + rddCombine.collect().mkString(","))//(k01,(29,2)),(k03,(2,1)),(k02,(6,1))
    val rddSubtract:RDD[(String,Int)] = rdd2.subtractByKey(other);
    println("====subtractByKey====:" + rddSubtract.collect().mkString(","))// (k03,2),(k02,6)
    val rddJoin:RDD[(String,(Int,Int))] = rdd2.join(other)
    println("=====rddJoin====:" + rddJoin.collect().mkString(","))// (k01,(3,29)),(k01,(26,29))
    val rddRight:RDD[(String,(Option[Int],Int))] = rdd2.rightOuterJoin(other)
    println("====rightOuterJoin=====:" + rddRight.collect().mkString(","))// (k01,(Some(3),29)),(k01,(Some(26),29))
    val rddLeft:RDD[(String,(Int,Option[Int]))] = rdd2.leftOuterJoin(other)
    println("=====rddLeft=====:" + rddLeft.collect().mkString(","))// (k01,(3,Some(29))),(k01,(26,Some(29))),(k03,(2,None)),(k02,(6,None))
    val rddCogroup: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd2.cogroup(other)
    println("=====cogroup=====:" + rddCogroup.collect().mkString(","))// (k01,(CompactBuffer(3, 26),CompactBuffer(29))),(k03,(CompactBuffer(2),CompactBuffer())),(k02,(CompactBuffer(6),CompactBuffer()))
    // 行动操作
    val resCountByKey = rdd2.countByKey()
    println("=====countByKey=====:" + resCountByKey)// Map(k01 -> 2, k03 -> 1, k02 -> 1)
    val resColMap = rdd2.collectAsMap()
    println("=====resColMap=====:" + resColMap)//Map(k02 -> 6, k01 -> 26, k03 -> 2)
    val resLookup = rdd2.lookup("k01")
    println("====lookup===:" + resLookup) // WrappedArray(3, 26)

    // 其他一些不常用的RDD操作
    println("=====first=====:" + rdd.first())//(k01,3)
    val resTop = rdd2.top(2).map(x => x._1 + ";" + x._2)
    println("=====top=====:" + resTop.mkString(","))// k03;2,k02;6
    val resTake = rdd2.take(2).map(x => x._1 + ";" + x._2)
    println("=======take====:" + resTake.mkString(","))// k01;3,k02;6
    val resTakeSample = rdd2.takeSample(false, 2).map(x => x._1 + ";" + x._2)
    println("=====takeSample====:" + resTakeSample.mkString(","))// k01;26,k03;2
    val resSample1 = rdd2.sample(false, 0.25)
    val resSample2 = rdd2.sample(false, 0.75)
    val resSample3 = rdd2.sample(false, 0.5)
    println("=====sample======:" + resSample1.collect().mkString(","))// 无
    println("=====sample======:" + resSample2.collect().mkString(","))// (k01,3),(k02,6),(k01,26)
    println("=====sample======:" + resSample3.collect().mkString(","))// (k01,3),(k01,26)



















  }


}
