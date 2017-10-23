package base.cache.spark

import java.util.concurrent.ConcurrentMap

import base.cache.Cache
import base.cache.components.CacheComponent
import org.apache.curator.shaded.com.google.common.collect.MapMaker
import org.apache.spark.rdd.RDD
import collection.JavaConverters._

class RDDCache extends CacheComponent[String, RDD[_]]
  with Cache[String, RDD[_]] {

  type T

  private[cache] val persistentValues = {
    val map: ConcurrentMap[String, RDD[T]] = new MapMaker().weakValues().makeMap[String, RDD[T]]()
    map.asScala
  }

  private def getOrElseNone[U](key: String)(f1: RDD[T] => U)(f2: => U) = persistentValues.get(key) match {
    case Some(rdd) => f1(rdd)
    case None => f2
  }

  override def appendData(key: String, value: RDD[_]): Unit = {
    getOrElseNone(key) {
      srcRDD: RDD[T] => {

        val newRDD = srcRDD.union(value.asInstanceOf[RDD[T]]).coalesce(srcRDD.getNumPartitions).cache()
        newRDD.count()
        srcRDD.unpersist()
        persistentValues.update(key, newRDD)
      }
    } {
      value.cache()
      value.count()
      persistentValues.put(key, value.asInstanceOf[RDD[T]])
    }
  }

  override def unCacheData(key: String): Unit = getOrElseNone(key) {
    srcRDD: RDD[_] => {
      srcRDD.unpersist()
      persistentValues.remove(key)
    }
  } {
    throw new IllegalArgumentException(s"cache for key:[$key] not exist")
  }

  override def getData(key: String): Option[RDD[_]] = getOrElseNone(key) {
    v: RDD[_] => Option(v)
  } {
    None
  }

  def getAll = persistentValues.values

}
