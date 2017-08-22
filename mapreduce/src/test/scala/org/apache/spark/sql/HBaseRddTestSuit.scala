package org.apache.spark.sql

import org.apache.arrow.memory.RootAllocator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Scan}
import org.apache.hadoop.hbase.mapreduce.{RowCounter, TableRecordReader}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.HBaseRDD
import org.apache.spark.util.SerializableConfiguration
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * Created by wpy on 17-7-16.
  */
class HBaseRddTestSuit extends FunSuite with BeforeAndAfter {

  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test_hbase")
  sparkConf.registerKryoClasses(Array(classOf[TableName], classOf[Scan], classOf[TableRecordReader], classOf[Configuration]))
  val sc = SparkContext.getOrCreate(sparkConf)
  val conf = HBaseConfiguration.create()
  val configuration = new SerializableConfiguration(conf)
  val conn = ConnectionFactory.createConnection(conf)
  val admin = conn.getAdmin
  val tableName = TableName.valueOf("wpy1:test")
  test("hbase rdd") {
    //    val scan = TableMapReduceUtil.convertScanToString(new Scan())
    val job =
      RowCounter.createSubmittableJob(new Configuration(), Array("wpy1:test"))
    //    val c = sc.broadcast(conf)
    val rdd = new HBaseRDD(sc, "wpy1:test", new SerializableConfiguration(job.getConfiguration), 100 * 1000 * 1000)
    val buf = new RootAllocator(Int.MaxValue).buffer(2048)
    val pars = rdd.zipWithIndex().map(cell => cell._1._2.rawCells().map(c => buf.readBytes(CellUtil.cloneValue(c))))

    rdd.foreach(_ => {})
    Console.in.read()
    //    while (true) Thread.sleep(10000)
  }

  test("put") {
    for (i <- 0 until 100000) {
      val rowKey = i.formatted("%08d").toString
      val put = new Put(Bytes.toBytes(rowKey))
      for (j <- 0 until 10) {
        put.addColumn(Bytes.toBytes("index"), Bytes.toBytes(s"idx_$j"), Bytes.toBytes(rowKey + "index"))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(s"cf_$j"), Bytes.toBytes(rowKey + "column_family"))
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes(s"usr_$j"), Bytes.toBytes(rowKey + "user"))
        conn.getTable(tableName).put(put)
      }
    }
    conn.close()
  }

  test("split") {
    val splits = new Array[Array[Byte]](20)
    for (i <- splits.indices) {
      splits(i) = Bytes.toBytes(((i + 1) * 5000).formatted("%08d"))
    }
    splits.foreach { split =>
      admin.split(tableName, split)
      Thread.sleep(1000)
    }
    admin.close()
    conn.close()
  }

  test("truncate") {
    admin.disableTable(tableName)
    admin.truncateTable(tableName, false)
    admin.close()
    conn.close()
  }
}
