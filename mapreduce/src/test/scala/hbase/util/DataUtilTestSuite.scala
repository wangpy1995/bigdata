package hbase.util

import org.apache.spark.SparkConf
import org.scalatest.FunSuite

class DataUtilTestSuite extends FunSuite {

  test("modify_tab") {
    val sparkConf = new SparkConf().setAppName("mod_table").setMaster("local[*]")
    val util = new DataUtil(sparkConf)
    util.copyRegion("/home/wpy/tmp/hbase/data/wpy1/test/43ba2d8fc153f44e81aa92cb8fbf65b9", "/tmp/mtable", "wpy1:test")
    while (true) Thread.sleep(10000)
  }
}
