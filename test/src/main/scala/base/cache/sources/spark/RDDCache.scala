package base.cache.sources.spark

import java.util.concurrent.ConcurrentMap

import base.cache.Cache
import base.cache.components.CacheComponent
import org.apache.curator.shaded.com.google.common.collect.MapMaker
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

class RDDCache extends CacheComponent[String, RDD[_]]
  with Cache {

  private[cache] val persistentValues = {
    val map: ConcurrentMap[K, V] = new MapMaker().weakValues().makeMap[K, V]()
    map.asScala
  }

  private def getOrElseNone[U](key: K)(f1: V => U)(f2: => U) = persistentValues.get(key) match {
    case Some(rdd) => f1(rdd)
    case None => f2
  }

  override def appendData(key: K, value: V): Unit = {
    getOrElseNone(key) {
      srcRDD: V => {
        val newRDD = srcRDD.union(value).coalesce(srcRDD.getNumPartitions).cache()
        newRDD.count()
        srcRDD.unpersist()
        persistentValues.update(key, newRDD)
      }
    } {
      value.cache()
      value.count()
      persistentValues.put(key, value)
    }
  }

  override def unCacheData(key: K): Unit = getOrElseNone(key) {
    srcRDD: V => {
      srcRDD.unpersist()
      persistentValues.remove(key)
    }
  } {
    throw new IllegalArgumentException(s"cache for key:[$key] not exist")
  }

  override def getData(key: K): Option[V] = getOrElseNone(key) {
    v: V => Option(v)
  } {
    None
  }

  def getAll = persistentValues.values
}
