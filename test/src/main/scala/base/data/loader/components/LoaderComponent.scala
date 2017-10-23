package base.data.loader.components

import base.data.loader.{Converter, Loader}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext

trait LoaderComponent {
  self: Loader =>
  def load(sc:SparkContext,conf:Configuration): V = self.doLoad(sc,conf)
}


trait LoadWithConverterComponent[T, U] {
  self: Loader with Converter =>
  override type V = T
  override type In = V
  override type Out = U

  def loadAndConvert(sc:SparkContext,conf:Configuration) = self.convert(self.doLoad(sc,conf))
}