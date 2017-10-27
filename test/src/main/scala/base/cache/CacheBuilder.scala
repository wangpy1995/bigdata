package base.cache

import java.util.{ServiceConfigurationError, ServiceLoader}

import base.cache.components.CacheComponent
import base.cache.sources.parquet.ParquetCacheCreator
import base.cache.sources.spark.RDDCacheCreator
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object CacheBuilder extends Logging {

  def buildCacheComponent[K, V](
                                 ss: SparkSession,
                                 className: String,
                                 keyTag: ClassTag[K],
                                 valueTag: ClassTag[V],
                                 options: Map[String, String] = Map.empty
                               ): Cache with CacheComponent[K, V] = lookupCreator(className).newInstance() match {
    case cc: CacheCreator[K,V] =>
      cc.createCache(ss, options)
    case _ =>
      throw new UnsupportedOperationException(s"unsupported")
  }

  /** A map to maintain backward compatibility in case we move data sources around. */
  private val backwardCompatibilityMap: Map[String, String] = {
    val rdd = classOf[RDDCacheCreator].getCanonicalName
    val parquet = classOf[ParquetCacheCreator].getCanonicalName

    Map(
      "base.cache.sources.spark.RDDCache" -> rdd,
      "base.cache.sources.spark" -> rdd,
      "base.cache.sources.parquet" -> parquet,
      "base.cache.sources.parquet.ParquetCache" -> parquet
    )
  }

  def lookupCreator(provider: String): Class[_] = {
    val provider1 = backwardCompatibilityMap.getOrElse(provider, provider)
    val provider2 = s"$provider1.DefaultSource"
    val loader = Option(Thread.currentThread().getContextClassLoader).getOrElse(CacheBuilder.getClass.getClassLoader)
    val serviceLoader = ServiceLoader.load(classOf[CacheCreator], loader)
    try {
      serviceLoader.asScala.filter(_.shortName().equalsIgnoreCase(provider1)).toList match {
        // the provider format did not match any given registered aliases
        case Nil =>
          try {
            Try(loader.loadClass(provider1)).orElse(Try(loader.loadClass(provider2))) match {
              case Success(dataSource) =>
                // Found the data source using fully qualified path
                dataSource
              case Failure(error) =>
                throw new ClassNotFoundException(
                  s"Failed to find cache: $provider1. ",
                  error)
            }
          } catch {
            case e: NoClassDefFoundError => // This one won't be caught by Scala NonFatal
              throw e
          }
        case head :: Nil =>
          head.getClass
        case sources =>
          val sourceNames = sources.map(_.getClass.getName)
          val internalSources = sources.filter(_.getClass.getName.startsWith("base.cache.sources"))
          if (internalSources.size == 1) {
            logWarning(s"Multiple caches found for $provider1 (${sourceNames.mkString(", ")}), " +
              s"defaulting to the internal cache (${internalSources.head.getClass.getName}).")
            internalSources.head.getClass
          } else {
            throw new UnsupportedOperationException(s"Multiple caches found for $provider1 " +
              s"(${sourceNames.mkString(", ")}), please specify the fully qualified class name.")
          }
      }
    } catch {
      case e: ServiceConfigurationError if e.getCause.isInstanceOf[NoClassDefFoundError] =>
        throw e
    }
  }
}