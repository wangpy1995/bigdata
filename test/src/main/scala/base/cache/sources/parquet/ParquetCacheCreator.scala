package base.cache.sources.parquet

import base.cache.CacheCreator
import org.apache.spark.sql.{DataFrame, SparkSession}

class ParquetCacheCreator extends CacheCreator[String,DataFrame] {
  override def shortName() = "parquet"

  override def createCache(ss: SparkSession, option: Map[String, String]) = {
    new ParquetCache(ss, option("name"), option("path"), option("partitionKey"))
  }
}
