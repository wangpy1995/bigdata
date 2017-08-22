package org.apache.spark.rdd

import java.io.IOException
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import mapreduce.utils.HBaseUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableInputFormatBase, TableRecordReader, TableSplit}
import org.apache.hadoop.hbase.util.{Bytes, RegionSizeCalculator}
import org.apache.hadoop.hbase.{HConstants, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.scheduler.{HDFSCacheTaskLocation, HostTaskLocation, SplitInfoReflections}
import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangpengyu6 on 2017/7/15.
  */
class HBaseRDD(sc: SparkContext,
                                  tableNameString: String,
                                  configuration: SerializableConfiguration,
                                  size: Long) extends RDD[(ImmutableBytesWritable, Result)](sc, Nil) {
  var tableRecordReader: TableRecordReader = _
  private val confBroadcast = sc.broadcast(configuration)
  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)
    formatter.format(new Date())
  }
  private val shouldCloneJobConf = sparkContext.getConf.getBoolean("spark.hadoop.cloneConf", false)

  def getConf: Configuration = {
    val conf: Configuration = confBroadcast.value.value
    if (shouldCloneJobConf) {
      HBaseRDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
        logDebug("Cloning Hadoop Configuration")
        if (conf.isInstanceOf[JobConf]) {
          new JobConf(conf)
        } else {
          new Configuration(conf)
        }
      }
    } else {
      conf
    }
  }

  private def createRecordReader(tSplit: TableSplit) = {
    val tableName = TableName.valueOf(tableNameString)
    val conf = configuration.value
    val conn = ConnectionFactory.createConnection(conf)
    val table = conn.getTable(tableName)
    val trr = if (this.tableRecordReader != null) this.tableRecordReader
    else new TableRecordReader
    val sc = new Scan(HBaseUtils.convertStringToScan(conf.get(TableInputFormat.SCAN)))
    sc.setStartRow(tSplit.getStartRow)
    sc.setStopRow(tSplit.getEndRow)
    logDebug(s"Scan: $sc, table: $table")
    trr.setScan(sc)
    trr.setTable(table)
    new RecordReader[ImmutableBytesWritable, Result]() {
      @throws[IOException]
      override def close(): Unit = {
        trr.close()
        table.close()
      }

      @throws[InterruptedException]
      @throws[IOException]
      override def getCurrentKey: ImmutableBytesWritable = trr.getCurrentKey

      @throws[InterruptedException]
      @throws[IOException]
      override def getCurrentValue: Result = trr.getCurrentValue

      @throws[InterruptedException]
      @throws[IOException]
      override def getProgress: Float = trr.getProgress

      @throws[InterruptedException]
      @throws[IOException]
      override def initialize(inputsplit: InputSplit, context: TaskAttemptContext): Unit = {
        trr.initialize(inputsplit, context)
      }

      @throws[InterruptedException]
      @throws[IOException]
      override def nextKeyValue: Boolean = trr.nextKeyValue
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(ImmutableBytesWritable, Result)] = {
    val iterator = new Iterator[(ImmutableBytesWritable, Result)] {
      val hbaseSplit = split.asInstanceOf[HBaseRegionPartition]
      private val _conf = getConf
      private val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
      private val hadoopAttemptContext = new TaskAttemptContextImpl(_conf, attemptId)
      private var reader = createRecordReader(hbaseSplit.serializableHadoopSplit.value)
      reader.initialize(hbaseSplit.serializableHadoopSplit.value, hadoopAttemptContext)
      private var finished = false
      private var havePair = false
      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener(_ => close())

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
    new InterruptibleIterator(context, iterator)
  }

  override protected def getPartitions: Array[Partition] = {
    val tableName = TableName.valueOf(tableNameString)
    val conf = configuration.value
    val conn = ConnectionFactory.createConnection(conf)
    //    val scan = HBaseUtils.convertStringToScan(s)
    val scan = new Scan(HBaseUtils.convertStringToScan(conf.get(TableInputFormat.SCAN)))
    val isText = conn.getConfiguration.getBoolean(TableInputFormatBase.TABLE_ROW_TEXTKEY, true)
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
      val startRow = scan.getStartRow
      val stopRow = scan.getStopRow

      val splitPair = new Array[(Array[Byte], Array[Byte])](keys.getFirst.length)
      val first = keys.getFirst
      val second = keys.getSecond
      for (i <- keys.getFirst.indices)
        if ((startRow.isEmpty || second(i).length == 0
          || Bytes.compareTo(startRow, second(i)) < 0)
          && (stopRow.isEmpty || Bytes.compareTo(stopRow, first(i)) > 0)) {
          val splitStart = if (startRow.isEmpty || Bytes.compareTo(first(i), startRow) >= 0)
            first(i)
          else startRow

          val splitStop = if ((stopRow.isEmpty || Bytes.compareTo(second(i), stopRow) <= 0)
            && second(i).length > 0) second(i)
          else stopRow

          splitPair(i) = (splitStart, splitStop)
        }
      var index = 0
      val parts = new ArrayBuffer[Partition]()
      splitPair.foreach { indexPair =>
        val splitStart = indexPair._1
        val splitStop = indexPair._2
        logDebug(s"split region: ============>(${Bytes.toString(splitStart)},${Bytes.toString(splitStop)})<============")
        val regionLocation = regionLocator.getRegionLocation(splitStart)
        val regionSize = regionSizeCalculator.getRegionSize(regionLocation.getRegionInfo.getRegionName)
        if (regionSize > size) {
          val num = (regionSize / size).toInt
          val splitKeys = getSplitKey(splitStart, splitStop, num, isText)
          for (i <- 0 until splitKeys.length - 1) {
            logDebug(s"splitKey_$index: ------------>(${splitKeys(i).map(_.toChar).mkString},${splitKeys(i + 1).map(_.toChar).mkString})<------------")
            parts += HBaseRegionPartition(index, new TableSplit(
              tableName,
              scan,
              splitKeys(i),
              splitKeys(i + 1),
              regionLocation.getHostname,
              size
            ))
            index += 1
          }
        }
        else {
          logDebug(s"SPLIT NO NEED:(${Bytes.toString(splitStart)}, ${Bytes.toString(splitStop)})")
          parts += HBaseRegionPartition(index, new TableSplit(tableName,
            scan,
            splitStart,
            splitStop,
            regionLocation.getHostname,
            regionSize))
          index += 1
        }
        parts
      }
      parts.toArray
    }
  }

  private def getSplitKey(start: Array[Byte], end: Array[Byte], num: Int, isText: Boolean): Array[Array[Byte]] = {
    val splitKeys = new ArrayBuffer[Array[Byte]]()
    val (upperLimitByte, lowerLimitByte): (Byte, Byte) = if (isText) ('~', ' ') else (-1, 0)
    // For special case
    // Example 1 : startkey=null, endkey="hhhqqqwww", splitKey="h"
    // Example 2 (text key mode): startKey="ffffaaa", endKey=null, splitkey="f~~~~~~"
    if (start.length == 0 && end.length == 0)
      for (i <- 1 to num)
        splitKeys += Array[Byte](((lowerLimitByte + upperLimitByte) / i).toByte)
    else if (start.length == 0 && end.length != 0)
      for (i <- 1 to num)
        splitKeys += Array((end(0) / i).toByte)
    else if (start.length != 0 && end.length == 0) {
      splitKeys += start
      for (i <- 1 to num) {
        val result = new Array[Byte](start.length)
        result(0) = (start(0) / i).toByte
        for (k <- 1 until start.length) result(k) = upperLimitByte
        splitKeys += result
      }
    }
    else Bytes.split(start, end, false, num).foreach(splitKeys += _)
    /* if (Bytes.compareTo(splitKeys.last, end) < 0)
       splitKeys += end*/
    splitKeys.toArray
  }

  override def getPreferredLocations(hsplit: Partition): Seq[String] = {
    val split = hsplit.asInstanceOf[HBaseRegionPartition].serializableHadoopSplit.value
    val locs = HBaseRDD.SPLIT_INFO_REFLECTIONS match {
      case Some(c) =>
        try {
          val infos = c.newGetLocationInfo.invoke(split).asInstanceOf[Array[AnyRef]]
          HBaseRDD.convertSplitLocationInfo(infos)
        } catch {
          case e: Exception =>
            logDebug("Failed to use InputSplit#getLocationInfo.", e)
            None
        }
      case None => None
    }
    locs.getOrElse(split.getLocations.filter(_ != "localhost"))
  }
}


object HBaseRDD extends Logging {
  private val SPLIT_INFO_REFLECTIONS: Option[SplitInfoReflections] = try {
    Some(new SplitInfoReflections)
  } catch {
    case e: Exception =>
      logDebug("SplitLocationInfo and other new Hadoop classes are " +
        "unavailable. Using the older Hadoop location info code.", e)
      None
  }
  private val CONFIGURATION_INSTANTIATION_LOCK = new Object()

  private def convertSplitLocationInfo(infos: Array[AnyRef]): Option[Seq[String]] = {
    Option(infos).map(_.flatMap { loc =>
      val reflections = HBaseRDD.SPLIT_INFO_REFLECTIONS.get
      val locationStr = reflections.getLocation.invoke(loc).asInstanceOf[String]
      if (locationStr != "localhost") {
        if (reflections.isInMemory.invoke(loc).asInstanceOf[Boolean]) {
          logDebug(s"Partition $locationStr is cached by Hadoop.")
          Some(HDFSCacheTaskLocation(locationStr).toString)
        } else {
          Some(HostTaskLocation(locationStr).toString)
        }
      } else {
        None
      }
    })
  }

  def create(sc: SparkContext,
             tableName: String,
             conf: Configuration,
             size: Long): HBaseRDD = {
    new HBaseRDD(sc, tableName,
      new SerializableConfiguration(conf),
      size)
  }

}

case class HBaseRegionPartition(index: Int, @transient split: TableSplit) extends Partition {
  val serializableHadoopSplit = new SerializableWritable(split)
}
