package org.apache.spark.rdd.scheduler

import mapreduce.utils.Utils

/**
  * Created by wangpengyu6 on 2017/7/17.
  */
class SplitInfoReflections {
  val inputSplitWithLocationInfo =
    Utils.classForName("org.apache.hadoop.mapred.InputSplitWithLocationInfo")
  val getLocationInfo = inputSplitWithLocationInfo.getMethod("getLocationInfo")
  val newInputSplit = Utils.classForName("org.apache.hadoop.mapreduce.InputSplit")
  val newGetLocationInfo = newInputSplit.getMethod("getLocationInfo")
  val splitLocationInfo = Utils.classForName("org.apache.hadoop.mapred.SplitLocationInfo")
  val isInMemory = splitLocationInfo.getMethod("isInMemory")
  val getLocation = splitLocationInfo.getMethod("getLocation")
}
