package spark_demo

object HelloWorld {
  def main(args: Array[String]) {
    println("Hello, world!") // 输出 Hello World
    val path=  this.getClass().getResource("sparkdata01.txt").toString.substring(6)
    println(path)
  }
}
