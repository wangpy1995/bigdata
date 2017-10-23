package base.data.test

import base.data.sources.DataLoader
import base.data.sources.hbase.HBaseLoader
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{IdentityTableMapper, TableMapReduceUtil}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

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
  test("hbase") {


    val loader = new HBaseLoader().loadAndConvert(sc, job.getConfiguration)
    val res = loader.collect()
    res
  }

  test("dataLoader") {
    val name = new HBaseLoader().getClass.getName
    val ss = SparkSession.builder().config(sparkConf).getOrCreate()
    val data = DataLoader(ss,name,job.getConfiguration).load()
    data
  }

}
