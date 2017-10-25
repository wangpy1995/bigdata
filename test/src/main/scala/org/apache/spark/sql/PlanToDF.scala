package org.apache.spark.sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.optimizer.CombineUnions
import org.apache.spark.sql.catalyst.plans.logical.Union

object PlanToDF {
  lazy val fs = FileSystem.get(new Configuration())

  def unionDF(sparkSession: SparkSession, df: Seq[DataFrame]) = {
    val logicalPlan = CombineUnions(Union(df.map(_.queryExecution.logical)))
    val qe = sparkSession.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    new Dataset[Row](sparkSession, qe, RowEncoder(qe.analyzed.schema))
  }

  def unCheckpoint(rdd: RDD[_]) = {
    rdd.context.cleaner.get.registerRDDCheckpointDataForCleanup(rdd.sparkContext.emptyRDD,rdd.id)
    rdd.checkpointData = None
  }
}
