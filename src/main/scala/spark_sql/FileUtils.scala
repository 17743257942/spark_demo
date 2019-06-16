package spark_sql
import java.io.File
object FileUtils {

//  def main(args: Array[String]) {
//    val file:File = new File("E:\\Program Files\\vm_linux\\share\\spark_sql_out")
//    dirDel(file)
//  }



  //遍历目录
  def subdirs(dir: File): Iterator[File] = {

    val children = dir.listFiles.filter(_.isDirectory())
    children.toIterator ++ children.toIterator.flatMap(subdirs _)

  }

  //删除目录和文件
  def dirDel(path: File) {
    if (!path.exists())
      return
    else if (path.isFile()) {
      path.delete()
//      println(path + ":  文件被删除")
      return
    }

    val file: Array[File] = path.listFiles()
    for (d <- file) {
      dirDel(d)
    }

    path.delete()
//    println(path + ":  目录被删除")

  }



}
