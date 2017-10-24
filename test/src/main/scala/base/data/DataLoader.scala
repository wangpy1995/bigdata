package base.data

import java.util.{ServiceConfigurationError, ServiceLoader}

import base.data.loader.sources.hbase.{HBaseLoader, HBaseLoaderCreator}
import base.data.loader.sources.kafka.{KafkaLoader, KafkaLoaderCreator}
import base.data.loader.{Converter, Loader, LoaderCreator, LoaderRegister}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case class DataLoader(
                       sparkSession: SparkSession,
                       className: String,
                       paths: Seq[String] = Nil,
                       options: Map[String, String] = Map.empty
                     ) extends Logging {
  lazy val providingClass: Class[_] = DataLoader.lookupDataLoader(className)

  def resolveLoader: Loader = {
    providingClass.newInstance() match {
      case l: LoaderCreator =>
        l.createLoader(sparkSession.sparkContext, options)
      case _ =>
        throw new UnsupportedOperationException("only LoaderCreator is supported in this version")
    }
  }

  def load[T](): T = {
    resolveLoader match {
      case lc: Loader with Converter =>
        lc.loadAs()
      case l: Loader =>
        l.loadAs()
      case provider =>
        throw new UnsupportedOperationException(s"not supported providing class[${provider.getClass.getName}]")
    }
  }

}

object DataLoader extends Logging {

  /** A map to maintain backward compatibility in case we move data sources around. */
  private val backwardCompatibilityMap: Map[String, String] = {
    val hbase = classOf[HBaseLoaderCreator].getCanonicalName
    val kafka = classOf[KafkaLoaderCreator].getCanonicalName

    Map(
      "org.apache.hbase" -> hbase,
      "base.data.loader.sources.hbase" -> hbase,
      "org.apache.kafka" -> kafka,
      "base.data.loader.sources.kafka" -> kafka
    )
  }

  def lookupDataLoader(provider: String): Class[_] = {
    val provider1 = backwardCompatibilityMap.getOrElse(provider, provider)
    val provider2 = s"$provider1.DefaultSource"
    val loader = Option(Thread.currentThread().getContextClassLoader).getOrElse(DataLoader.getClass.getClassLoader)
    val serviceLoader = ServiceLoader.load(classOf[LoaderRegister], loader)

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
                  s"Failed to find data source: $provider1. Please find packages at " +
                    "http://spark.apache.org/third-party-projects.html",
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
          val internalSources = sources.filter(_.getClass.getName.startsWith("org.apache.spark"))
          if (internalSources.size == 1) {
            logWarning(s"Multiple sources found for $provider1 (${sourceNames.mkString(", ")}), " +
              s"defaulting to the internal datasource (${internalSources.head.getClass.getName}).")
            internalSources.head.getClass
          } else {
            throw new UnsupportedOperationException(s"Multiple sources found for $provider1 " +
              s"(${sourceNames.mkString(", ")}), please specify the fully qualified class name.")
          }
      }
    } catch {
      case e: ServiceConfigurationError if e.getCause.isInstanceOf[NoClassDefFoundError] =>
        throw e
    }
  }
}