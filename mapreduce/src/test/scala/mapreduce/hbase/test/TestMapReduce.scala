package mapreduce.hbase.test

import java.io.{ByteArrayOutputStream, File, FileInputStream}
import java.util.Scanner

/**
  * Created by wpy on 2017/6/21.
  */
object TestMapReduce {
  def main(args: Array[String]): Unit = {

    val scanner = new Scanner(System.in)
    @volatile var cls: ClassLoader = null
    while (true) {
      val input = scanner.next()
      cls = /*this.getClass.getClassLoader.loadClass*/ new MyClassLoader("/home/wpy/IdeaProjects/bigdata/out/production/test/")
      //      cls.newInstance()
      val loadClass = cls.loadClass(s"test.$input")
      loadClass.getDeclaredMethods
        .foreach(m => println(m.getName))
      cls = null
    }
  }
}

class MyClassLoader(classPath: String) extends ClassLoader {

  override def findClass(name: String): Class[_] = {
    println(s"load class: $name")
    val data = loadClassData(name)
    this.defineClass(name, data, 0, data.length)
  }

  def loadClassData(name: String): Array[Byte] = {
    val newName = name.replace(".", "/")
    val in = new FileInputStream(new File(classPath + newName + ".class"))
    val output = new ByteArrayOutputStream()
    var b = in.read()
    while (b != -1) {
      output.write(b)
      b = in.read()
    }
    in.close()
    output.toByteArray
  }
}