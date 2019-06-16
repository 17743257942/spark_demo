package spark_sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * 转换工具类  输入变输出
  */
object ConvertUtil {


  /**
    * 定义结果输出类型
    */
  val struct = StructType(
    Array(
      StructField("url", StringType),
      StructField("type", StringType),
      StructField("id", LongType),
      StructField("nums", LongType),
      StructField("ip", StringType),
      StructField("city", StringType),
      StructField("time", StringType),
      StructField("day", StringType)
    )
  )

  /**
    * 根据输入的每一行记录转换成输出的样式
    */
  def parseLog(log: String) = {

    try {
      val splits = log.split("\002")

      val url = splits(1)
      val nums = splits(2).toLong
      val ip = splits(3)

      val domain = "http://www.imooc.com/"
      val typeid0 = url.substring(url.indexOf(domain) + domain.length)
      val typeid = typeid0.split("/")
      var type1 = ""
      var id = 0l
      if (typeid.length > 1) {
        type1 = typeid(0)
        id = typeid(1).replaceAll("\"", "").toLong
      }

      val city = ""
      val time = splits(0)
      val day = time.substring(0, 10).replaceAll("-", "")

      //Row里面的字段要和struct对应
      Row(url, type1, id, nums, ip, city, time, day)
    } catch {
      case e: Exception => Row("err", "err", 0l, 0l, "err", "err", "err", "err")
    }


  }


}
