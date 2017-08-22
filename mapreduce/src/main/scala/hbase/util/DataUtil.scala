package hbase.util

import java.util.UUID

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.backup.mapreduce.MapReduceHFileSplitterJob
import org.apache.hadoop.hbase.io.hfile.{CacheConfig, HFile, HFileContext}
import org.apache.hadoop.hbase.mapreduce.HFileInputFormat
import org.apache.hadoop.hbase.regionserver.StoreFile
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.NullWritable
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

class DataUtil(sparkConf: SparkConf) extends Serializable with Logging {

  implicit object OrderingCell extends Ordering[Cell] {
    override def compare(x: Cell, y: Cell): Int = CellComparator.COMPARATOR.compare(x, y)
  }

  def copyRegion(input: String, output: String, tabName: String) = {
    val conf = HBaseConfiguration.create()
    conf.set(MapReduceHFileSplitterJob.BULK_OUTPUT_CONF_KEY, output)

    val job = new MapReduceHFileSplitterJob()
    job.setConf(conf)
    job.createSubmittableJob(Array(input, tabName))

    SparkContext.getOrCreate(sparkConf).newAPIHadoopFile(input, classOf[HFileInputFormat], classOf[NullWritable], classOf[Cell], job.getConf)
      .mapPartitions { kvs =>
        val set = new mutable.TreeSet[Cell]()(OrderingCell)
        kvs.foreach { kv =>
          val cell = kv._2
          val row = Bytes.toString(CellUtil.cloneRow(cell))
          val r = Bytes.toBytes("2017" + row)
          val put = new KeyValue(r, CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell))
          put.setTimestamp(cell.getTimestamp)
          set += put
        }
        set.iterator
      }.foreachPartition { cells =>
      val writerMap = new mutable.HashMap[String, HFile.Writer]()
      val configuration = SparkContext.getOrCreate().hadoopConfiguration
      cells.foreach { cell =>
        val family = Bytes.toString(CellUtil.cloneFamily(cell))
        writerMap.getOrElseUpdate(family, {
          val fileContext = new HFileContext()
          val path = new Path(output, family)
          val familyDir = new Path(path, UUID.randomUUID.toString.replaceAll("-", ""))
          HFile.getWriterFactory(configuration, CacheConfig.DISABLED)
            .withComparator(CellComparator.COMPARATOR)
            .withFileContext(fileContext)
            .withPath(path.getFileSystem(configuration), familyDir)
            .create()
        }).append(cell)
      }
      writerMap.values.filter(_ != null).foreach { w =>
        logInfo("Writer=" + w.getPath)
        w.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY,
          Bytes.toBytes(System.currentTimeMillis()))
        w.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY,
          Bytes.toBytes(true))
        w.appendFileInfo(StoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY,
          Bytes.toBytes(false))
        w.close()
      }
    }
  }
}
