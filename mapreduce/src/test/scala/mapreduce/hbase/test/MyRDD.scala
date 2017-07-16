package mapreduce.hbase.test

import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wpy on 17-7-7.
  */
class MyRDD[K, V](sc: SparkContext, rdd: NewHadoopRDD[K, V], numParts: Int) extends RDD[(K, V)](sc, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
    val s = split.asInstanceOf[MyPartition]
    s.array.flatMap(p => rdd.compute(p, context)).iterator
  }

  override protected def getPartitions: Array[Partition] = {
    val partitions = new Array[MyPartition](numParts)
    for (i <- partitions.indices) partitions(i) = MyPartition(ArrayBuffer.empty, i)
    for (p <- rdd.getPartitions.zipWithIndex) {
      partitions(p._2 % numParts).array += p._1
    }
    partitions.asInstanceOf[Array[Partition]]
  }
}

case class MyPartition(array: ArrayBuffer[Partition], var index: Int) extends Partition


object TestMyRDD extends Logging {
  def main(args: Array[String]): Unit = {
    val num = 5
    val sparkConf = new SparkConf().setAppName("test_my_rdd").setMaster("local[*]")
    sparkConf.registerKryoClasses(Array(classOf[ImmutableBytesWritable]))
    val sc = SparkContext.getOrCreate(sparkConf)
    //    val job = Job.getInstance()
    val conf = HBaseConfiguration.create()
//    val conn = ConnectionFactory.createConnection(conf)
//    import collection.JavaConverters._
//    for (region <- conn.getAdmin.getTableRegions(TableName.valueOf("wpy:test")).asScala){
//      println((Bytes.toString(region.getStartKey),Bytes.toString(region.getEndKey)))
//    }
    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("cf"))
    scan.withStartRow(Bytes.toBytes("071"))
    scan.withStopRow(Bytes.toBytes("097"))
//    val scanner = conn.getTable(TableName.valueOf("wpy:test")).getScanner(scan).iterator()
//    while (scanner.hasNext){
//      println(scanner.next().rawCells().map(CellUtil.cloneValue).map(Bytes.toString).mkString("\t"))
//    }
//    System.exit(0)
    val serializedScan = Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
    conf.set("hbase.mapreduce.scan", serializedScan)
    conf.set("hbase.mapreduce.inputtable", "wpy:test")
    //    TableMapReduceUtil.initTableMapperJob("wpy:test", new Scan(), classOf[IdentityTableMapper], classOf[ImmutableBytesWritable], classOf[Result], job)
    val rdd = new NewHadoopRDD(sc, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result], conf)
    val h = rdd.partitions.length
    val hc = rdd.coalesce(num).map(_._2.rawCells().map(CellUtil.cloneValue).map(Bytes.toString).mkString("\t")).foreach(println)
    println(hc + "\t" + h + "\t")
    val myRDD = new MyRDD(sc, rdd, num)
    val n = myRDD.partitions.length
    val c = myRDD.map(_._2.rawCells().map(CellUtil.cloneValue).map(Bytes.toString).mkString("\t")).foreach(println)
    println(n + "\t" + c)
    while (true) {
      Thread.sleep(10000)
    }
  }
}