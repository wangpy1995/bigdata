package base.cache

trait Cache[K,V] {

  def appendData(key:K, value:V):Unit

  def unCacheData(key:K)

  def getData(key:K):Option[V]
}


