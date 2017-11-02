package org.apache.spark.rdd.arrow

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import scala.io.StdIn

class ArrowWriterTestSuite extends FunSuite{

  private lazy val sparkConf = new SparkConf().setAppName("arrow").setMaster("local[*]")
  private lazy val ss = SparkSession.builder().config(sparkConf).getOrCreate()

  test("arrow_writer"){
    import ss.implicits._
    (1 to 1600).toDF/*.collectAsArrowToPython()*/
    StdIn.readLine()
  }
}
