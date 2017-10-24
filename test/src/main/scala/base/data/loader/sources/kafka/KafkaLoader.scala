package base.data.loader.sources.kafka

import java.util.Properties

import base.data.loader.components.LoadWithConverterComponent
import base.data.loader.{Converter, Loader, LoaderCreator, LoaderRegister}
import base.data.{CacheRDD, KafkaRDD}
import org.apache.spark.SparkContext


class KafkaLoaderCreator extends LoaderCreator with LoaderRegister {
  override def createLoader(sc: SparkContext, options: Map[String, String]) = ???

  override def shortName() = "kafka"
}

/**
  * 用于创建RDD并读取kafka数据
  *
  * @param kafkaParam
  */
case class KafkaLoader(
                        kafkaParam: Properties
                      ) extends LoadWithConverterComponent[KafkaRDD, CacheRDD]
  with Loader with Converter {
  override def doLoad(): KafkaRDD = ???

  override def convert[U](src: In) = ???

  override def loadAs[T]() = loadAndConvert().asInstanceOf[T]
}
