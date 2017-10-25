package base.data.loader.sources.hbase

import java.io.{ObjectInputStream, ObjectOutputStream}

import base.data.loader.components.LoadWithConverterComponent
import base.data.loader.{Converter, Loader}
import base.data.{CacheRDD, HBaseRDD}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext

import scala.collection.mutable

case class HBaseLoader(
                        _sc: SparkContext,
                        conf: Configuration) extends LoadWithConverterComponent[HBaseRDD, CacheRDD]
  with Loader with Converter with KryoSerializable with Serializable {

  @transient var map = new mutable.HashMap[String, Array[Byte]]()

  override def doLoad(): HBaseRDD = {
    _sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
  }

  override def convert[U >: CacheRDD](src: In): U = {
    val bm = _sc.broadcast(map)
    src.mapPartitions { iter =>
      val m = bm.value
      iter.foreach {
        case (_, v) =>
          val cells = v.rawCells()
          cells.foreach(c => m += Bytes.toString(CellUtil.cloneQualifier(c)) -> CellUtil.cloneValue(c))
      }
      Iterator(m)
    }
  }

  override def read(kryo: Kryo, input: Input) = {
    map = kryo.readObject(input, classOf[mutable.HashMap[String, Array[Byte]]])
  }

  override def write(kryo: Kryo, output: Output) = {
    kryo.writeClass(output, classOf[mutable.HashMap[String, Array[Byte]]])
  }

  private def readObject(in: ObjectInputStream): Unit = {
    map = in.readObject().asInstanceOf[mutable.HashMap[String, Array[Byte]]]
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(map)
  }

  override def loadAs[T]() = loadAndConvert().asInstanceOf[T]
}
