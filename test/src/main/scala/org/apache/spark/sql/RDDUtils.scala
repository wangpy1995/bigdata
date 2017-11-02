package org.apache.spark.sql

import org.apache.spark.Dependency
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{RDD, ReliableRDDCheckpointData}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.optimizer.CombineUnions
import org.apache.spark.sql.catalyst.plans.logical.Union

import scala.collection.mutable

object RDDUtils extends Logging {

  val rddDep = new mutable.HashMap[Int, Seq[Dependency[_]]]()

  def unionDF(sparkSession: SparkSession, df: Seq[DataFrame]) = {
    val logicalPlan = CombineUnions(Union(df.map(_.queryExecution.logical)))
    val qe = sparkSession.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    new Dataset[Row](sparkSession, qe, RowEncoder(qe.analyzed.schema))
  }

  //保存rdd的依赖关系
  def saveDependency(rdd: RDD[_]) = rddDep.getOrElseUpdate(rdd.id, rdd.dependencies)

  def unCheckpoint(rdd: RDD[_]) = rddDep.get(rdd.id) match {
    case Some(dep) =>
      cleanCheckpointFile(rdd)
      val clz = getSuperRDDClass(rdd.getClass)
      val field = clz.getDeclaredField("org$apache$spark$rdd$RDD$$dependencies_")
      //粗爆的改成可访问，不管现有修饰
      field.setAccessible(true)
      field.set(rdd, dep)
      //依赖已经恢复,从map中移除
      rddDep.remove(rdd.id)
      rdd.checkpointData = None
      logInfo("clear checkpoint data success, remove dependencies")
    case None =>
      logInfo("rdd依赖未保存,强行删除checkpoint文件会导致rdd数据丢失," +
        "强行删除checkpoint请使用{ RDDUtils.cleanCheckpoint(rdd: RDD[_]) }")
  }

  def clean(rdd: RDD[_]) = {
    cleanCheckpointFile(rdd)
    val clz = getSuperRDDClass(rdd.getClass)
    val field = clz.getDeclaredField("org$apache$spark$rdd$RDD$$dependencies_")
    //粗爆的改成可访问，不管现有修饰
    field.setAccessible(true)
    field.set(rdd, Seq.empty[Dependency[_]])
    //从map中移除
    rddDep.remove(rdd.id)
    rdd.checkpointData = None
    logInfo("clear checkpoint data success, remove dependencies")
  }

  private def cleanCheckpointFile(rdd: RDD[_]) = {
    ReliableRDDCheckpointData.cleanCheckpoint(rdd.sparkContext, rdd.id)
  }

  private def getSuperRDDClass(clazz: Class[_]): Class[_] = {
    if (clazz.getSuperclass.getSuperclass != null)
      getSuperRDDClass(clazz.getSuperclass)
    else
      clazz
  }

}
