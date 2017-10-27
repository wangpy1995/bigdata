package base.cache

import base.cache.components.CacheComponent
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

trait Cache {
  type K
  type V

  def overwriteData(key: K, value: V)

  def appendData(key: K, value: List[V]): Unit

  def unCacheData(key: K)

  def getData(key: K): Option[List[V]]
}

trait CacheCreator[K,V] {
  def shortName(): String

  def createCache(
                   ss: SparkSession, option: Map[String, String]
                 ): Cache with CacheComponent[K, V]
}