package base.cache

import org.apache.spark.sql.SparkSession

trait Cache {
  type K
  type V

  def appendData(key: K, value: V): Unit

  def unCacheData(key: K)

  def getData(key: K): Option[V]
}

trait CacheCreator {
  def createCache(ss: SparkSession, option: Map[String, String]): Cache
}