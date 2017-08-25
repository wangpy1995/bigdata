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

  test("split") {
    val start = "201711844_123dsa56"
    val end = "201711910_asdsada"

    def getSplitKeys(start: String, end: String, num: Int): Seq[String] = {
      val (s, sHash) = (start.substring(0, 6), start.substring(6, 9).toInt)
      val (e, eHash) = (end.substring(0, 6), end.substring(6, 9).toInt)

      val upper = if (eHash % 1000 == 0) 1000 else eHash
      Seq(start) ++ (for (i <- 1 to num) yield {
        sHash + i * ((upper - sHash) / (num + 1))
      }).map(s + _) ++ Seq(end)
    }

    getSplitKeys(start, end, 3).foreach(println)
  }


}
