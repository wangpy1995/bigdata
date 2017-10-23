package base.data.loader

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext

trait Loader {
  type V

  def doLoad(sc:SparkContext,conf:Configuration): V
}

trait Converter {
  type In
  type Out
  def convert(src: In): Out
}
