package base.data.loader.sources.hbase

import base.data.loader.{LoaderCreator, LoaderRegister}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext

class HBaseLoaderCreator extends LoaderCreator with LoaderRegister {
  override def shortName() = "hbase"

  override def createLoader(sc: SparkContext, options: Map[String, String]) = {
    val conf = new Configuration()
    options.foreach(kv => conf.set(kv._1, kv._2))
    HBaseLoader(sc, conf)
  }
}
