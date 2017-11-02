package base.cache.sources.parquet

import base.cache.Cache
import base.cache.components.CacheComponent
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, ExpressionSet, GreaterThanOrEqual, InSet, IsNotNull, LessThanOrEqual, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{TableIdentifier, expressions}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

class ParquetCache(
                    _ss: SparkSession,
                    name: String,
                    path: String,
                    partitionKey: String
                  ) extends CacheComponent[String, DataFrame] with Cache {
  /**
    *
    * @param key   partition Key
    * @param value table data
    */
  override def appendData(key: K, value: List[V]): Unit = {
    val dfWriter = RDDUtils.unionDF(_ss, value).coalesce(1).write.mode(SaveMode.Append)
    (if (partitionKey.nonEmpty) dfWriter.partitionBy(partitionKey)
    else dfWriter).parquet(path)
    Table.refresh()
  }

  override def unCacheData(key: K): Unit = {
    FileSystem.get(Table.conf).delete(new Path(path), true)
    Table.df = null
  }

  override def overwriteData(key: String, value: V): Unit = {
    val dfWriter = value.coalesce(1).write.mode(SaveMode.Overwrite)
    (if (partitionKey.nonEmpty) dfWriter.partitionBy(partitionKey)
    else dfWriter).parquet(path)
    Table.refresh()
  }

  override def getData(key: K) = Some(Table.df :: Nil)

  object Table extends Logging {
    var df: DataFrame = _
    var plan: LogicalPlan = _
    var output: Seq[Attribute] = _
    lazy val conf = _ss.sparkContext.hadoopConfiguration

    def refresh() = {
      df = _ss.read.parquet(path)
      plan = Table.df.queryExecution.optimizedPlan
      output = Table.plan.output
    }

    /**
      * not serialized shut cache with another map operation
      *
      * @param partitionKey
      * @param start
      * @param end
      * @param dataKey
      * @param values
      * @return
      */
    def load[T](
                 partitionKey: String,
                 start: T, end: T,
                 dataKey: String = "classifier",
                 values: Array[Int] = Array.empty[Int]
               ) = {
      if (df == null || df.schema.isEmpty) {
        refresh()
      }
      SparkSession.setActiveSession(_ss)

      // Partition keys are not available in the statistics of the files.
      val partitionKeyFilters = if (partitionKey.nonEmpty) {
        val partitionAttr = output.find(_.name == partitionKey).get
        ExpressionSet(Seq(IsNotNull(partitionAttr),
          LessThanOrEqual(partitionAttr, Literal(end, partitionAttr.dataType)),
          GreaterThanOrEqual(partitionAttr, Literal(start, partitionAttr.dataType))))
      } else ExpressionSet(Seq.empty[Expression])

      logInfo(s"Pruning directories with: ${partitionKeyFilters.mkString(",")}")
      val dataFilters = if (values.nonEmpty) {
        val dataAttr = output.find(_.name == dataKey).get
        Seq(InSet(dataAttr, values.toSet[Any]))
      }
      else Seq.empty[Expression]

      UserDefinedFileSourceStrategy(plan, partitionKeyFilters, dataFilters).head.execute()
    }
  }

}

object UserDefinedFileSourceStrategy extends Logging {
  def apply(plan: LogicalPlan, partitionKeyFilters: ExpressionSet, dataFilters: Seq[Expression]) = plan match {
    case PhysicalOperation(projects, _,
    l@LogicalRelation(fsRelation: HadoopFsRelation, _, _, _)) =>
      val partitionColumns =
        l.resolve(
          fsRelation.partitionSchema, fsRelation.sparkSession.sessionState.analyzer.resolver)

      val dataColumns =
        l.resolve(fsRelation.dataSchema, fsRelation.sparkSession.sessionState.analyzer.resolver)

      // Predicates with both partition keys and attributes need to be evaluated after the scan.
      val afterScanFilters = dataFilters

      logInfo(s"Post-Scan Filters: ${afterScanFilters.mkString(",")}")

      val filterAttributes = AttributeSet(afterScanFilters)
      val requiredExpressions: Seq[NamedExpression] = filterAttributes.toSeq ++ projects
      val requiredAttributes = AttributeSet(requiredExpressions.diff(partitionKeyFilters.toSeq))

      val readDataColumns =
        dataColumns
          .filter(requiredAttributes.contains)
          .filterNot(partitionColumns.contains)
      val outputSchema = readDataColumns.toStructType
      logInfo(s"Output Data Schema: ${outputSchema.simpleString(5)}")

      val outputAttributes = readDataColumns ++ partitionColumns

      val scan =
        FileSourceScanExec(
          fsRelation,
          outputAttributes,
          outputSchema,
          partitionKeyFilters.toSeq,
          dataFilters,
          Some(TableIdentifier("")))

      val afterScanFilter = afterScanFilters.reduceOption(expressions.And)
      val withFilter = afterScanFilter.map(execution.FilterExec(_, scan)).getOrElse(scan)
      val withProjections = if (projects == withFilter.output) {
        withFilter
      } else {
        execution.ProjectExec(projects, withFilter)
      }

      withProjections :: Nil

    case _ => Nil
  }

}