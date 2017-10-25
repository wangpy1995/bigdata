package base.cache.sources.spark

import base.cache.CacheCreator
import org.apache.spark.sql.SparkSession

class RDDCacheCreator extends CacheCreator {
  override def createCache(ss: SparkSession, option: Map[String, String]) =
    new RDDCache()

  override def shortName() = "rdd"
}