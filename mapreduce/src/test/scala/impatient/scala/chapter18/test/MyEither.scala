package impatient.scala.chapter18.test

import scala.util.control.Breaks

/**
  * Created by wpy on 17-7-1.
  */
object MyEither {

  type ->[A, B] = (A, B)

  def process(x: Array[Int], y: Int) = {
    var dif = Int.MaxValue
    var j = 0
    var k = 0
    //    val loop = new Breaks
    Breaks.tryBreakable {
      for (i <- x.indices) {
        val z = math.abs(y - x(i))
        if (z == 0) {
          k = i
          j = -1
        }
        else if (z < dif) {
          dif = z
          j = i
          k = -1
        }
        else Breaks.break()
      }
    } catchBreak {}
    j -> k
  }

  def main(args: Array[String]): Unit = {
    println(process(Array(0, 1, 2, 3, 4, 5, 6), 3))
  }
}
