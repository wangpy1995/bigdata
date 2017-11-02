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
import org.apache.hadoop.mapreduce.lib.input.{CombineFileSplit, FileSplit}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.scheduler.{HDFSCacheTaskLocation, HostTaskLocation, SplitInfoReflections}
import org.apache.spark.util.{SerializableConfiguration, ShutdownHookManager}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangpengyu6 on 2017/7/15.
  */
class HBaseRDD(
                sc: SparkContext,
                tableNameString: String,
                configuration: SerializableConfiguration,
                val size: Long
              ) extends RDD[(ImmutableBytesWritable, Result)](sc, Nil) {
  var tableRecordReader: TableRecordReader = _
  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)
    formatter.format(new Date())
  }
  private val shouldCloneJobConf = sparkContext.getConf.getBoolean("spark.hadoop.cloneConf", defaultValue = false)

  def getConf: Configuration = {
    val conf: Configuration = configuration.value
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

      logInfo("Input split: " + hbaseSplit.serializableHadoopSplit)
      private val inputMetrics = context.taskMetrics().inputMetrics
      private val existingBytesRead = inputMetrics.bytesRead

      // Sets InputFileBlockHolder for the file block's information
      hbaseSplit.serializableHadoopSplit.value match {
        case ts: TableSplit =>
          InputFileBlockHolder.set(ts.getRegionLocation, 0L, ts.getLength)
        case _ =>
          InputFileBlockHolder.unset()
      }

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // creating RecordReader, because RecordReader's constructor might read some bytes
      private val getBytesReadCallback: Option[() => Long] =
      hbaseSplit.serializableHadoopSplit.value match {
        case t: TableSplit =>
          Some(() => t.getLength)
        case _ => None
      }

      // We get our input bytes from thread-local Hadoop FileSystem statistics.
      // If we do a coalesce, however, we are likely to compute multiple partitions in the same
      // task and in the same thread, in which case we need to avoid override values written by
      // previous partitions (SPARK-13071).
      private def updateBytesRead(): Unit = {
        getBytesReadCallback.foreach { getBytesRead =>
          inputMetrics.setBytesRead(existingBytesRead + getBytesRead())
        }
      }


      private val _conf = getConf
      private val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
      private val hadoopAttemptContext = new TaskAttemptContextImpl(_conf, attemptId)
      private var reader = createRecordReader(hbaseSplit.serializableHadoopSplit.value)
      reader.initialize(hbaseSplit.serializableHadoopSplit.value, hadoopAttemptContext)
      private var finished = false
      private var havePair = false
      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener { _ =>
        updateBytesRead()
        close()
      }

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
        if (!finished) {
          inputMetrics.incRecordsRead(1)
        }
        if (inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
          updateBytesRead()
        }
        (reader.getCurrentKey, reader.getCurrentValue)
      }

      private def close() = {
        if (reader != null) {
          InputFileBlockHolder.unset()
          try {
            reader.close()
          } catch {
            case e: Exception =>
              if (!ShutdownHookManager.inShutdown()) {
                logWarning("Exception in RecordReader.close()", e)
              }
          } finally {
            reader = null
          }
          if (getBytesReadCallback.isDefined) {
            updateBytesRead()
          } else if (hbaseSplit.serializableHadoopSplit.value.isInstanceOf[TableSplit]) {
            // If we can't get the bytes read from the FS stats, fall back to the split size,
            // which may be inaccurate.
            try {
              inputMetrics.incBytesRead(hbaseSplit.serializableHadoopSplit.value.getLength)
            } catch {
              case e: java.io.IOException =>
                logWarning("Unable to get input size to set InputMetrics for task", e)
            }
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
      val parts = new ArrayBuffer[HBaseRegionPartition]()
      splitPair.foreach {
        indexPair =>
          val splitStart = indexPair._1
          val splitStop = indexPair._2
          logDebug(s"split region: ============>(${
            Bytes.toString(splitStart)
          },${
            Bytes.toString(splitStop)
          })<============")
          val regionLocation = regionLocator.getRegionLocation(splitStart)
          val regionSize = regionSizeCalculator.getRegionSize(regionLocation.getRegionInfo.getRegionName)
          if (regionSize > size * 1.2) {
            val n = regionSize / size
            val num = if (n > 1) n else n + 1

            val splitKeys = getSplitKey(splitStart, splitStop, num.toInt, isText)
            for (i <- 0 until splitKeys.length - 1) {
              logInfo(s"splitKey_$index: ------------>(${
                splitKeys(i).map(_.toChar).mkString
              },${
                splitKeys(i + 1).map(_.toChar).mkString
              })<------------")
              if (i < splitKeys.length - 2)
                parts += HBaseRegionPartition(index, new TableSplit(
                  tableName,
                  scan,
                  splitKeys(i),
                  splitKeys(i + 1),
                  regionLocation.getHostname,
                  size
                ))
              else
                parts += HBaseRegionPartition(index, new TableSplit(
                  tableName,
                  scan,
                  splitKeys(i),
                  splitKeys(i + 1),
                  regionLocation.getHostname,
                  regionSize - size * (i + 1)))
              index += 1
            }
          }
          else {
            logInfo(s"SPLIT NO NEED:(${
              Bytes.toString(splitStart)
            }, ${
              Bytes.toString(splitStop)
            })")
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
      parts.toArray.filter(_.split.getLength > 0)
        .zipWithIndex.map {
        case (s, i) => HBaseRegionPartition(i, s.split)
      }.asInstanceOf[Array[Partition]]
    }
  }

  private def getSplitKey(start: Array[Byte], stop: Array[Byte], num: Int, isText: Boolean): Array[Array[Byte]] = {
    //后三位hash
    val splitKeys = new ArrayBuffer[Array[Byte]]()
    splitKeys += start
    val op = Bytes.toString(start)
    val ed = Bytes.toString(stop)
    val (startHash, stopHash) = (
      (if (op.length >= 9)
        op.substring(6, 9)
      else
        "000").toFloat,
      (if (ed.length >= 9)
        ed.substring(6, 9)
      else
        "999").toFloat
    )
    val splits = (if (startHash.toInt > stopHash.toInt) {
      val c: Float = (stopHash + 1000 - startHash) / num
      (1 until num).map {
        n =>
          val sp = (startHash + c * n).toInt
          if (sp > 999)
            ed.substring(0, 6) + (sp - 1000).formatted("%03d")
          else
            op.substring(0, 6) + sp.formatted("%03d")
      }
    } else {
      val c: Float = (stopHash - startHash) / num
      (1 until num).map(n => op.substring(0, 6) + (startHash + c * n).toInt.formatted("%03d"))
    }).foreach(s => splitKeys += Bytes.toBytes(s))
    splitKeys += stop
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

  override def toString: String = s"index: [$index]" + split.toString
}
