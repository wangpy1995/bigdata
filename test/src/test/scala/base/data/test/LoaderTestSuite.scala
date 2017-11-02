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
      "test",
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
    data foreach println
    StdIn.readLine()
  }

  test("cache") {
    val ss = SparkSession.builder().config(sparkConf).getOrCreate()
    ss.sparkContext.setCheckpointDir("/home/wangpengyu6/tmp/checkpoint")
    import ss.implicits._
    /* val opts = job.getConfiguration.iterator().asScala.map(kv => kv.getKey -> kv.getValue).toMap
     val testData1: CacheRDD = DataLoader(ss, "base.data.loader.sources.hbase", opts).load()
     val testData2: CacheRDD = DataLoader(ss, "base.data.loader.sources.hbase", opts).load()
     val testData3: CacheRDD = DataLoader(ss, "base.data.loader.sources.hbase", opts).load()*/

    val testData1 = Array((1, 2), (3, 4), (5, 6), (7, 8), (9, 0), (1, 2), (3, 4), (5, 6), (7, 8), (9, 0), (1, 2), (3, 4), (5, 6), (7, 8), (9, 0), (1, 2), (3, 4), (5, 6), (7, 8), (9, 0))
    val testData2 = Array((11, 12), (13, 14), (15, 16), (17, 18), (19, 10), (11, 12), (13, 14), (15, 16), (17, 18), (19, 10), (11, 12), (13, 14), (15, 16), (17, 18), (19, 10))
    val testData3 = Array((21, 22), (23, 24), (25, 26), (27, 28), (29, 20), (21, 22), (23, 24), (25, 26), (27, 28), (29, 20), (21, 22), (23, 24), (25, 26), (27, 28), (29, 20))
    val loader = Thread.currentThread().getContextClassLoader
    val k = loader.loadClass("java.lang.String")
    val v1 = loader.loadClass("org.apache.spark.rdd.RDD")
    val v2 = loader.loadClass("org.apache.spark.sql.Dataset")

    val l1Cache = CacheBuilder.buildCacheComponent(
      ss,
      "base.cache.sources.spark.RDDCacheCreator",
      classOf[String],
      classOf[RDD[(Int,Int)]])

    val l2Cache = CacheBuilder.buildCacheComponent(
      ss,
      "base.cache.sources.parquet.ParquetCacheCreator",
      classOf[String],
      classOf[DataFrame],
      Map("name" -> "data", "path" -> "/home/wangpengyu6/tmp/test_parquet", "partitionKey" -> "")
    )

    val testData = List(testData1, testData2, testData3).map(ss.sparkContext.parallelize(_))
    l1Cache.appendData("data", testData)
    l1Cache.getData("data") match {
      case Some(rdds) =>
        new UnionRDD(ss.sparkContext, rdds).coalesce(1).mapPartitions(iter => iter).distinct().foreach(println)
      case None =>
        println("none L1 cache")
    }
    l1Cache.unCache("data")

    l2Cache.appendData("data", testData.map(_.toDF("key", "value")))
    l2Cache.getData("data") /*Table.load("dt", 20170701, 20171030)*/ match {
      case Some(dfs) =>
        dfs.foreach(df => println(df.count()))
      case _ =>
        throw new Exception("unknown error")
    }
    //    l2Cache.unCache("data")

    StdIn.readLine()
  }

  test("createTable") {
    val hbaseUtil = new HBaseTestUtil
    hbaseUtil.loadFromHFile("test", "/home/wangpengyu6/tmp/hfile")
    //            hbaseUtil.truncateTable("test",false)
    hbaseUtil.close
  }
}
