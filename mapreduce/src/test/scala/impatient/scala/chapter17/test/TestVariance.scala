package impatient.scala.chapter17.test

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


