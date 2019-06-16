package spark_sql

object Test {
  def main(args: Array[String]): Unit = {

    val s: String = "-"
    println(s.indexOf("/"))
    if (s.indexOf("/") > 0) {
      println(s.substring(s.indexOf("/")))
    }

    println(s.split("/").foreach(println))

    val s2 = "http://www.imooc.com/code/1852"
    println(s2.indexOf("ttp"))

  }
}
