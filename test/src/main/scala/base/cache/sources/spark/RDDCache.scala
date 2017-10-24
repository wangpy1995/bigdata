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
    val map: ConcurrentMap[K, List[V]] = new MapMaker().weakValues().makeMap[K, List[V]]()
    map.asScala
  }

  private def getOrElseNone[T, U](key: K)(f1: T => U)(f2: => U) = persistentValues.get(key) match {
    case Some(rdd) => f1(rdd)
    case None => f2
  }

  override def appendData(key: K, values: List[V]): Unit = {
    getOrElseNone(key) {
      (oldRDDs: List[V]) => {
        val newRDDs = values.map(_.cache())
        newRDDs.foreach(_.count())
        persistentValues.update(key, oldRDDs ::: newRDDs)
      }
    } {
      val newRDDs = values.map(_.cache())
      newRDDs.foreach(_.count())
      persistentValues.put(key, newRDDs)
    }
  }

  override def unCacheData(key: K): Unit = getOrElseNone(key) {
    srcRDD: List[V] => {
      srcRDD.foreach(_.unpersist())
      persistentValues.remove(key)
    }
  } {
    throw new IllegalArgumentException(s"cache for key:[$key] not exist")
  }

  override def getData(key: K) = getOrElseNone(key) {
    v: List[V] => Option(v)
  } {
    None
  }

  def getAll = persistentValues.values
}
