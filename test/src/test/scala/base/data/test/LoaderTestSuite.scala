package base.data.test

import base.data.{CacheRDD, DataLoader}
import base.data.loader.sources.hbase.HBaseLoader
import base.data.test.utils.HBaseTestUtil
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{IdentityTableMapper, TableMapReduceUtil}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
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
  test("hbase") {
    val loader = HBaseLoader(sc, job.getConfiguration).loadAndConvert()
    val res = loader.collect()
    res
  }

  test("dataLoader") {
    import collection.JavaConverters._
    val ss = SparkSession.builder().config(sparkConf).getOrCreate()
    val opts = job.getConfiguration.iterator().asScala.map(kv => kv.getKey -> kv.getValue).toMap
    val data: CacheRDD = DataLoader(ss, "base.data.loader.sources.hbase", Nil, opts).load()
    data.collect().foreach(println)
    println(data.count())
    StdIn.readLine()
  }

  test("createTable") {
    val hbaseUtil = new HBaseTestUtil
    hbaseUtil.loadFromHFile("test", "/home/wangpengyu6/tmp/hfile")
//            hbaseUtil.truncateTable("test",false)
    hbaseUtil.close
  }

}
