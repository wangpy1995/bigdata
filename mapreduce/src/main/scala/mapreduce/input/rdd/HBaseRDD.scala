package mapreduce.input.rdd

import java.io.IOException

import org.apache.hadoop.hbase.client.{Connection, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableRecordReader, TableSplit}
import org.apache.hadoop.hbase.util.{Bytes, RegionSizeCalculator}
import org.apache.hadoop.hbase.{HConstants, TableName}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.spark._
import org.apache.spark.rdd.RDD

/**
  * Created by wangpengyu6 on 2017/7/15.
  */
class HBaseRDD(sc: SparkContext,
               tableName: TableName, conn: Connection,
               scan: Scan, size: Long) extends RDD[(ImmutableBytesWritable, Result)](sc, Nil) {
  val table = conn.getTable(tableName)
  val tableRecordReader = new TableRecordReader

  def createRecordReader(tSplit: TableSplit) = {
    val trr = if (this.tableRecordReader != null) this.tableRecordReader
    else new TableRecordReader
    val sc = new Scan(this.scan)
    sc.setStartRow(tSplit.getStartRow)
    sc.setStopRow(tSplit.getEndRow)
    trr.setScan(sc)
    trr.setTable(table)
    new RecordReader[ImmutableBytesWritable, Result]() {
      @throws[IOException]
      override def close(): Unit = {
        trr.close()
        table.close()
      }

      @throws[InterruptedException]
      override def getCurrentKey: ImmutableBytesWritable = trr.getCurrentKey

      @throws[InterruptedException]
      override def getCurrentValue: Result = trr.getCurrentValue

      @throws[InterruptedException]
      override def getProgress: Float = trr.getProgress

      @throws[InterruptedException]
      override def initialize(inputsplit: InputSplit, context: TaskAttemptContext): Unit = {
        trr.initialize(inputsplit, context)
      }

      @throws[InterruptedException]
      override def nextKeyValue: Boolean = trr.nextKeyValue
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(ImmutableBytesWritable, Result)] = {
    val iter = new Iterator[(ImmutableBytesWritable, Result)] {
      val hbaseSplit = split.asInstanceOf[HBaseRegionPartition]
      private var reader = createRecordReader(hbaseSplit.split)
      private var finished = false
      private var havePair = false
      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener(context => close())

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          try {
            finished = !reader.nextKeyValue
          } catch {
            case e: IOException =>
              logWarning(
                s"Skipped the rest content in the corrupted file: ${hbaseSplit.serializableHadoopSplit}",
                e)
              finished = true
          }
          if (finished) {
            // Close and release the reader here; close() will also be called when the task
            // completes, but for tasks that read from many files, it helps to release the
            // resources early.
            close()
          }
          havePair = !finished
        }
        !finished
      }

      override def next(): (ImmutableBytesWritable, Result) = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        (reader.getCurrentKey, reader.getCurrentValue)
      }


      private def close() = {
        if (reader != null) {
          try {
            reader.close()
          } catch {
            case e: Exception =>
              logWarning("Exception in RecordReader.close()", e)
          } finally {
            reader = null
          }
        }
      }
    }
    new InterruptibleIterator(context, iter)
  }

  override protected def getPartitions: Array[Partition] = {
    val admin = conn.getAdmin
    val regionLocator = conn.getRegionLocator(tableName)
    val regionSizeCalculator = new RegionSizeCalculator(regionLocator, admin)
    val keys = regionLocator.getStartEndKeys
    if (keys == null || keys.getFirst == null || keys.getFirst.length == 0) {
      val regLoc = regionLocator.getRegionLocation(HConstants.EMPTY_BYTE_ARRAY, false)
      val regionSize = regionSizeCalculator.getRegionSize(regLoc.getRegionInfo.getRegionName)
      //start:null,end:null========>(~~~~~~~,~~~~~~~)
      Array(HBaseRegionPartition(0, new TableSplit(tableName,
        scan,
        HConstants.EMPTY_BYTE_ARRAY,
        HConstants.EMPTY_BYTE_ARRAY,
        regLoc.getHostname,
        regionSize)))
    }
    else {
      val start = scan.getStartRow
      val stop = scan.getStopRow

      val splitPair = (for (i <- keys.getFirst.indices
                            if Bytes.compareTo(keys.getFirst()(i), start) > 0 &&
                              Bytes.compareTo(keys.getSecond()(i), stop) < 0)
        yield (keys.getFirst()(i), keys.getSecond()(i))).toArray

      splitPair(0) = (start, splitPair.head._2)
      splitPair(splitPair.length - 1) = (splitPair.last._2, stop)
      splitPair.zipWithIndex.flatMap { indexPair =>
        val splitStart = indexPair._1._1
        val splitStop = indexPair._1._2
        val regionLocation = regionLocator.getRegionLocation(splitStart)
        val regionSize = regionSizeCalculator.getRegionSize(regionLocation.getRegionInfo.getRegionName)
        if (regionSize > size) {
          var s = splitStart
          val num = (regionSize / size).toInt
          val maxLen = if (splitStart.length > splitStop.length) splitStart.length else splitStop.length
          val splitKey = new Array[Byte](maxLen)
          ((for (i <- 0 until num) yield {
            for (j <- 0 until maxLen) {
              splitKey(j) =
                if (splitStart(j) == splitStop(j))
                  splitStart(j)
                else if (j < splitStart.length)
                  ((i + 1) * (splitStop(j) - splitStart(j)) / num).toByte
                else ((i + 1) * splitStop(j) / num).toByte
            }
            val regionPartition = HBaseRegionPartition(i, new TableSplit(
              tableName,
              scan,
              s,
              splitKey,
              regionLocation.getHostname,
              size
            ))
            s = splitKey
            regionPartition
          }) ++ Seq(HBaseRegionPartition(num, new TableSplit(tableName, scan,
            s,
            splitStop,
            regionLocation.getHostname,
            regionSize - num * size)))).toArray
        }
        else Array(HBaseRegionPartition(indexPair._2, new TableSplit(tableName,
          scan,
          splitStart,
          splitStop,
          regionLocation.getHostname,
          regionSize)))
      }
    }
  }

}

case class HBaseRegionPartition(index: Int, split: TableSplit) extends Partition {
  val serializableHadoopSplit = new SerializableWritable(split)
}
