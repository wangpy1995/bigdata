package impatient.scala.chapter18.test

/**
  * Created by wpy on 17-7-2.
  */
object TestApply {

  def printValue(f: Int => Int, from: Int, to: Int): Unit = {
    println((for (i <- from to to) yield f(i)).mkString(" "))
  }


  def main(args: Array[String]): Unit = {
    printValue(Array(1, 2, 3, 4, 5, 6), 1, 3)
    println(new Seconds(1) + new Meters(2))
  }
}

abstract class Dim[T](val value: Double, val name: String) {
  //  self: T =>
  protected def create(v: Double): T

  def + (other: Dim[T]) = create(value + other.value)

  override def toString: String = value + name
}

class Seconds(v: Double) extends Dim[Seconds](v, "s") {
  override protected def create(v: Double): Seconds = new Seconds(v)
}

class Meters(v: Double) extends Dim[Seconds](v, "m") {
  override protected def create(v: Double): Seconds = new Seconds(v)
}