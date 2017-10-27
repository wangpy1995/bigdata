package base.cache.components

import base.cache.Cache
import org.apache.hadoop.io.SecureIOUtils.AlreadyExistsException

sealed abstract class CacheMode

private[cache] case object Append extends CacheMode

private[cache] case object Overwrite extends CacheMode

private[cache] case object ErrorIfExists extends CacheMode

private[cache] case object Ignore extends CacheMode

object CacheModes {
  def Append = base.cache.components.Append

  def Overwrite = base.cache.components.Overwrite

  def ErrorIfExists = base.cache.components.ErrorIfExists

  def Ignore = base.cache.components.Ignore
}

trait CacheComponent[T,U]{
  self: Cache =>
  override type K = T
  override type V = U

  def cache(key: K, value: V, mode: CacheMode = ErrorIfExists) = mode match {
    case Append =>
      appendData(key, value :: Nil)
    case Overwrite =>
      overwriteData(key,value)
    case ErrorIfExists =>
      getData(key) match {
        case Some(v) =>
          throw new AlreadyExistsException(s"key:[$key]->value:[$v] 已存在!")
        case None => appendData(key, value :: Nil)
      }
    case Ignore =>
  }

  def unCache(key: K) = unCacheData(key)
}
