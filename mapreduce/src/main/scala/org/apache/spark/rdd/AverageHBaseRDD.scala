package org.apache.spark.rdd

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.{Partition, TaskContext}

import scala.collection.mutable.ArrayBuffer

class AverageHBaseRDD(rdd: HBaseRDD) extends RDD[(ImmutableBytesWritable, Result)](rdd.sparkContext, rdd.dependencies) {
  override def compute(split: Partition, context: TaskContext) = {
    val avergeSplit = split.asInstanceOf[AverageHBasePartition]
    avergeSplit.partition.flatMap(rdd.compute(_,context)).iterator
  }

  override protected def getPartitions = {
    val res = new ArrayBuffer[Array[HBaseRegionPartition]]()
    val temp = ArrayBuffer.empty[HBaseRegionPartition]
    val tt = (rdd.size * 1.15).toLong

    val parts = rdd.partitions.asInstanceOf[Array[HBaseRegionPartition]]
    val theSmalls = parts.filter(_.split.getLength < tt * 0.8).sortBy(_.split.getLength)
    val theLarges = parts.diff(theSmalls).map(Array(_))

    def func(total: Long, sortedData: Array[HBaseRegionPartition]): Unit = {
      if (sortedData.length > 1) {
        val t = sortedData.head.split.getLength
        if (t + sortedData.last.split.getLength < total) {
          temp += sortedData.head
          func(total - t, sortedData.drop(1))
        } else if (sortedData.head.split.getLength + sortedData.last.split.getLength == total) {
          temp += sortedData.head += sortedData.last
          res += temp.toArray
          temp.clear()
          func(tt, sortedData.drop(1).dropRight(1))
        } else {
          temp += sortedData.last
          res += temp.toArray
          temp.clear()
          func(tt, sortedData.dropRight(1))
        }
      }
    }

    func(tt, theSmalls)

    (res ++ theLarges).zipWithIndex.map {
      case (partitions, i) => AverageHBasePartition(i, partitions)
    }.toArray
  }
}

case class AverageHBasePartition(index: Int, partition: Array[HBaseRegionPartition]) extends Partition