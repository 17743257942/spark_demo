package spark_sql

import java.text.SimpleDateFormat
import java.util.{Date, Locale}


object DateUtils {


  val Z_TIME_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
  val TARGET_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  //  [10/Nov/2016:00:01:02 +0800] ==> yyyy-MM-dd HH:mm:ss
  def parse(time: String) = {
    TARGET_FORMAT.format(new Date(getTimeL(time)))

  }

  //  Z类型时间转成long类型
  def getTimeL(time: String) = {
    try {
      val s = time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))
      Z_TIME_FORMAT.parse(s).getTime
    } catch {
      case e: Exception => {
        0l
      }
    }
  }

//  def main(args: Array[String]): Unit = {
//    println(parse("[10/Nov/2016:00:01:02 +0800]"))
//  }


}
