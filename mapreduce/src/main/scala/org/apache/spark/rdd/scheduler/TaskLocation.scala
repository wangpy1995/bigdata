package org.apache.spark.rdd.scheduler

/**
  * Created by wangpengyu6 on 2017/7/17.
  */
sealed trait TaskLocation {
  def host: String
}

private object TaskLocation {
  // We identify hosts on which the block is cached with this prefix.  Because this prefix contains
  // underscores, which are not legal characters in hostnames, there should be no potential for
  // confusion.  See  RFC 952 and RFC 1123 for information about the format of hostnames.
  val inMemoryLocationTag = "hdfs_cache_"

  // Identify locations of executors with this prefix.
  val executorLocationTag = "executor_"

  def apply(host: String, executorId: String): TaskLocation = {
    new ExecutorCacheTaskLocation(host, executorId)
  }

  /**
    * Create a TaskLocation from a string returned by getPreferredLocations.
    * These strings have the form executor_[hostname]_[executorid], [hostname], or
    * hdfs_cache_[hostname], depending on whether the location is cached.
    */
  def apply(str: String): TaskLocation = {
    val hstr = str.stripPrefix(inMemoryLocationTag)
    if (hstr.equals(str)) {
      if (str.startsWith(executorLocationTag)) {
        val hostAndExecutorId = str.stripPrefix(executorLocationTag)
        val splits = hostAndExecutorId.split("_", 2)
        require(splits.length == 2, "Illegal executor location format: " + str)
        val Array(host, executorId) = splits
        new ExecutorCacheTaskLocation(host, executorId)
      } else {
        new HostTaskLocation(str)
      }
    } else {
      new HDFSCacheTaskLocation(hstr)
    }
  }
}

private[hikvision] case class ExecutorCacheTaskLocation(override val host: String, executorId: String)
  extends TaskLocation {
  override def toString: String = s"${TaskLocation.executorLocationTag}${host}_$executorId"
}
private[hikvision] case class HDFSCacheTaskLocation(override val host: String) extends TaskLocation {
  override def toString: String = TaskLocation.inMemoryLocationTag + host
}

private[hikvision] case class HostTaskLocation(override val host: String) extends TaskLocation {
  override def toString: String = host
}