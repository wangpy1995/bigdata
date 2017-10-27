package base.cache.sources.spark

import base.cache.CacheCreator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class RDDCacheCreator extends CacheCreator[String,RDD[_]] {

  override def shortName() = "rdd"

  override def createCache(ss: SparkSession, option: Map[String, String]) = {
    new RDDCache()
  }
}