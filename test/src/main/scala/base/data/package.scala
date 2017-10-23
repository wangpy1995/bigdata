package base

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD

import scala.collection.mutable

package object data {

  type HBaseRDD = RDD[(ImmutableBytesWritable, Result)]

  type CacheRDD = RDD[mutable.Map[String,Any]]

}
