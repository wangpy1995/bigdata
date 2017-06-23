package mapreduce.input

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}

/**
  * Created by wpy on 2017/6/21.
  */
class TextInputFormat extends FileInputFormat[NullWritable, Text] {
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[NullWritable, Text] = ???
}

object TextRecordReader extends RecordReader[NullWritable, Text] {

  var in: Reader = _
  var conf: Configuration = _

  override def getCurrentKey: NullWritable = ???

  override def getProgress: Float = ???

  override def nextKeyValue(): Boolean = ???

  override def getCurrentValue: Text = ???

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val fileSplit = split.asInstanceOf[FileSplit]
    conf = context.getConfiguration
    val path = fileSplit.getPath
    val fs = path.getFileSystem(conf)
//    info(s"Initialize TextRecordReader for $path")
    this.in = null
  }

  override def close(): Unit = ???
}

class Reader