package base.data.loader

import org.apache.spark.SparkContext

trait LoaderRegister {
  def shortName():String
}

trait LoaderCreator{
  def createLoader(sc:SparkContext,options:Map[String,String]):Loader
}