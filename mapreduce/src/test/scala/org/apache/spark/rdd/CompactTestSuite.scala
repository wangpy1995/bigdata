package org.apache.spark.rdd

import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

class CompactTestSuite extends FunSuite {
  var testData = Array(1, 2, 5, 7, 8, 9, 2, 1, 4, 5, 6, 6, 6, 5, 5, 5, 7, 5, 6).sorted
  val res = new ArrayBuffer[Array[Int]]()
  val temp = ArrayBuffer.empty[Int]
  val tt = 13

  def func(total: Int, data: Array[Int]): Unit = {
    if (data.length > 1) {
      val t = data.head
      if (t + data.last < total) {
        temp += data.head
        func(total - t, data.drop(1))
      } else if (data.head + data.last == total) {
        temp += data.head += data.last
        res += temp.toArray
        temp.clear()
        func(tt, data.drop(1).dropRight(1))
      } else {
        temp += data.last
        res += temp.toArray
        temp.clear()
        func(tt, data.dropRight(1))
      }
    }
  }

  test("test"){
    func(11,testData)
    res.foreach(arr=>println(s"[ ${arr.mkString(",")} ]"))
  }
}
