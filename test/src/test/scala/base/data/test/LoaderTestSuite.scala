package base.data.test

import base.cache.CacheBuilder
import base.data.loader.sources.hbase.HBaseLoader
import base.data.test.utils.HBaseTestUtil
import base.data.{CacheRDD, DataLoader}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{IdentityTableMapper, TableMapReduceUtil}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

import scala.io.StdIn

class LoaderTestSuite extends FunSuite {

  lazy val sparkConf = new SparkConf().setMaster("local[*]").setAppName("base")


  lazy val sc = {
    sparkConf.registerKryoClasses(Array(classOf[HBaseLoader]))
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    SparkContext.getOrCreate(sparkConf)
  }
  lazy val job = {
    val j = Job.getInstance()
    val scan = new Scan()
    TableMapReduceUtil.initTableMapperJob(
      "wpy1:test",
      scan,
      classOf[IdentityTableMapper],
      classOf[ImmutableBytesWritable],
      classOf[Result], j)
    j
  }

  test("loader") {
    import collection.JavaConverters._
    val ss = SparkSession.builder().config(sparkConf).getOrCreate()
    val opts = job.getConfiguration.iterator().asScala.map(kv => kv.getKey -> kv.getValue).toMap
    val data: CacheRDD = DataLoader(ss, "base.data.loader.sources.hbase", opts).load()
    StdIn.readLine()
  }

  test("cache") {
    val ss = SparkSession.builder().config(sparkConf).getOrCreate()
    ss.sparkContext.setCheckpointDir("/home/wpy/tmp/checkpoint")
    import ss.implicits._
    val testData1 = ss.sparkContext.parallelize(Array((1, 2), (3, 4), (5, 6), (7, 8), (9, 0), (1, 2), (3, 4), (5, 6), (7, 8), (9, 0), (1, 2), (3, 4), (5, 6), (7, 8), (9, 0), (1, 2), (3, 4), (5, 6), (7, 8), (9, 0)))
    val testData2 = ss.sparkContext.parallelize(Array((11, 12), (13, 14), (15, 16), (17, 18), (19, 10), (11, 12), (13, 14), (15, 16), (17, 18), (19, 10), (11, 12), (13, 14), (15, 16), (17, 18), (19, 10)))
    val testData3 = ss.sparkContext.parallelize(Array((21, 22), (23, 24), (25, 26), (27, 28), (29, 20), (21, 22), (23, 24), (25, 26), (27, 28), (29, 20), (21, 22), (23, 24), (25, 26), (27, 28), (29, 20)))
    val l1Cache = CacheBuilder(ss, "base.cache.sources.spark.RDDCacheCreator").buildCacheComponent[String, RDD[(Int, Int)]]
    val l2Cache = CacheBuilder(
      ss,
      "base.cache.sources.parquet.ParquetCacheCreator",
      Map("name" -> "data", "path" -> "/home/wpy/tmp/test_parquet", "partitionKey" -> "")
    ).buildCacheComponent[String, DataFrame]

    l1Cache.appendData("data", testData1 :: testData2 :: testData3 :: Nil)
    l1Cache.getData("data") match {
      case Some(rdds) =>
        new UnionRDD(ss.sparkContext, rdds).coalesce(1).mapPartitions(iter => iter).distinct().foreach(println)
      case None =>
        println("none L1 cache")
    }
    l1Cache.unCache("data")
    l2Cache.appendData("data", (testData1 :: testData2 :: testData3 :: Nil).map(_.toDF("key", "value")))
    l2Cache.getData("data") match {
      case Some(dfs) => dfs.foreach(_.coalesce(1).distinct().map(_.toString).foreach(println(_)))
      case _ => throw new Exception("unknown error")
    }
    l2Cache.unCache("data")
    StdIn.readLine()
  }

  test("createTable") {
    val hbaseUtil = new HBaseTestUtil
    hbaseUtil.loadFromHFile("test", "/home/wangpengyu6/tmp/hfile")
    //            hbaseUtil.truncateTable("test",false)
    hbaseUtil.close
  }
}
