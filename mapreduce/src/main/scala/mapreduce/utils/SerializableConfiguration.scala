package mapreduce.utils

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.conf.Configuration

/**
  * Created by wangpengyu6 on 2017/7/17.
  */
private[mapreduce] class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    value = new Configuration(false)
    value.readFields(in)
  }
}