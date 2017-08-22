package impatient.scala.chapter17.test

import java.io._

import org.apache.hadoop.hbase.mapreduce.TableSplit
import org.apache.spark.SerializableWritable
import org.scalatest.FunSuite

/**
  * Created by wpy on 2017/6/23.
  */
object TestVariance {

  def mkFriends(a: Student, b: Friend[Student]) {
    b.beFriends(a)
  }

  def findStudent(person: Person): Student = {
    person.asInstanceOf[Student]
  }

  def friends(students: Array[Student], find: Person => Student): Array[Person] = {
    for (stu <- students) yield find(stu)
  }

  def main(args: Array[String]): Unit = {
    //    mkFriends(new Student("a"), new Person("b"))
    val students = new Array[Student](3)
    for (i <- students.indices) students(i) = new Student("Student: " + i.toString)
    friends(students, findStudent).foreach(s => println(s.name))
  }
}

class Person(val name: String) extends Friend[Person] {
  override def beFriends(someone: Person): Unit =
    println(this.getClass.getSimpleName + ": " + name + " & " + this.getClass.getSimpleName + ": " + someone.name + " is friends now")
}

class Student(name: String) extends Person(name)

trait Friend[-T] {
  def beFriends(someone: T)
}

class TestPos extends FunSuite {

  test("position") {
  }

  test("serialize") {
    //    val tableSplit = new TableSplit(TableName.valueOf("serializableWritable"),
    //      new Scan(),
    //      Bytes.toBytes("start"),
    //      Bytes.toBytes("end"),"local")
    //    val part = HBaseRegionPartition(1,tableSplit)

    //    val split = part.serializableHadoopSplit
    val file = new File("serialize")
    //    if(!file.exists())file.createNewFile()
    val in = new FileInputStream(file)
    //    val out = new FileOutputStream(file)
    //    new ObjectOutputStream(out).writeObject(split)

    val tableSplit = new ObjectInputStream(in).readObject().asInstanceOf[SerializableWritable[TableSplit]].value
    println(tableSplit)
    in.close()
  }
}
