package base.cache.sources.parquet

import base.cache.{Cache, CacheCreator}
import org.apache.spark.sql.SparkSession

class ParquetCacheCreator extends CacheCreator {
  override def createCache(ss: SparkSession, option: Map[String, String]): Cache = {
    new ParquetCache(ss, option("name"), option("path"), option("partitionKey"))
  }

  override def shortName() = "parquet"
}
