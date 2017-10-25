package base.data.test

import base.cache.CacheBuilder
import base.cache.components.CacheModes
import base.data.loader.sources.hbase.HBaseLoader
import base.data.test.utils.HBaseTestUtil
import base.data.{CacheRDD, DataLoader}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{IdentityTableMapper, TableMapReduceUtil}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
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
    StdIn.readLine()
  }

  test("cache") {
    val ss = SparkSession.builder().config(sparkConf).getOrCreate()
    import ss.implicits._
    val testData = ss.sparkContext.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 0))
    val l1Cache = CacheBuilder(ss, "base.cache.sources.spark.RDDCacheCreator").buildCacheComponent[String, RDD[Int]]
    val l2Cache = CacheBuilder(
      ss,
      "base.cache.sources.parquet.ParquetCacheCreator",
      Map("name" -> "data", "path" -> "/home/wangpengyu6/tmp/test_parquet", "partitionKey" -> "key")
    ).buildCacheComponent[String, DataFrame]


    l1Cache.cache("data", testData, CacheModes.Append)
    l2Cache.cache("data", testData.toDF("key"), CacheModes.Overwrite)
    l1Cache.getData("data") match {
      case Some(rdd) => rdd.foreach(_.mapPartitions(iter => iter).distinct().foreach(println))
      case None => println("none L1 cache")
    }
    l2Cache.getData("data") match {
      case Some(rdd) => rdd.foreach(_.map(_.toString).foreach(println(_)))
      case _ => throw new Exception("unknown error")
    }
    l1Cache.getData("data") match {
      case Some(rdd) => rdd.foreach(_.mapPartitions(iter => iter).distinct().foreach(println))
      case None => println("none L1 cache")
    }

    StdIn.readLine()
  }

  test("createTable") {
    val hbaseUtil = new HBaseTestUtil
    hbaseUtil.loadFromHFile("test", "/home/wangpengyu6/tmp/hfile")
    //            hbaseUtil.truncateTable("test",false)
    hbaseUtil.close
  }

}
