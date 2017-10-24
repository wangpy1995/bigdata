package base.master

import base.cache.Cache
import base.cache.components.{CacheComponent, CacheModes}
import org.apache.spark.internal.Logging

import scala.collection.mutable

trait Manager[T] {

  def getCachedData(key: String): List[T]

}

//TODO 考虑观察者模式??
class DataManager[T] extends Manager[T] with Logging {

  val l1Cache = new mutable.HashMap[String, CacheComponent[String, T] with Cache]()
  val l2Cache = new mutable.HashMap[String, CacheComponent[String, T] with Cache]()

  override def getCachedData(key: String): (List[T], List[T]) =
    (l1Cache(key).getData(key).get, l2Cache(key).getData(key).get)


  def addL1Cache(key: String, value: T) = l1Cache.get(key) match {
    case Some(cache) =>
      cache.cache(key, value, CacheModes.Append)
    case None =>
      throw new UnsupportedOperationException(s"no cache found for key:[$key]")
  }

  def addL2Cache(key: String, value: T) = l2Cache.get(key) match {
    case Some(cache) =>
      cache.cache(key, value, CacheModes.Append)
    case None =>
      throw new UnsupportedOperationException(s"no cache found for key:[$key]")
  }

  def initL1Cache(key: String, cache: CacheComponent[String, T] with Cache) =
    l1Cache.getOrElseUpdate(key,
      {
        logInfo(s"L1 cache for key:[$key] does not exists, do put")
        cache
      })

  def initL2Cache(key: String, cache: CacheComponent[String, T] with Cache) =
    l2Cache.getOrElseUpdate(key,
      {
        logInfo(s"L2 cache for key:[$key] does not exists, do put")
        cache
      })

}
