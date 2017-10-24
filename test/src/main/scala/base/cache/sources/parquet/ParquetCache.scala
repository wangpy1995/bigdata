package base.cache.sources.parquet

import base.cache.Cache
import base.cache.components.CacheComponent
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{TableIdentifier, expressions}
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Expression, ExpressionSet, GreaterThanOrEqual, InSet, IsNotNull, LessThanOrEqual, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, execution}

class ParquetCache(
                    _ss: SparkSession,
                    name: String,
                    path: String,
                    partitionKey: String
                  ) extends CacheComponent[String, DataFrame] with Cache {

  import Table._

  /**
    *
    * @param key   partition Key
    * @param value table data
    */
  override def appendData(key: K, value: List[V]): Unit =
    value.foreach(_.write.mode(SaveMode.Append).partitionBy(partitionKey).parquet(path))


  override def unCacheData(key: K): Unit = {
    _ss.sessionState.catalog.dropGlobalTempView(name)
    FileSystem.get(Table.conf).delete(new Path(path), true)
  }

  override def getData(key: K): Option[List[V]] = Some(df :: Nil)

  private object Table extends Logging {
    val df = _ss.read.parquet(path)
    df.createOrReplaceGlobalTempView(name)

    lazy val conf = df.sparkSession.sparkContext.hadoopConfiguration

    val table = _ss.sessionState.catalog.getTempViewOrPermanentTableMetadata(TableIdentifier(name))

    val plan = df.queryExecution.optimizedPlan
    val output = plan.output


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
    def load(
              partitionKey: String,
              start: String, end: String,
              dataKey: String = "classifier",
              values: Array[Int] = Array.empty[Int]
            ) = {
      val partitionAttr = output.filter(_.name == partitionKey).head
      val dataAttr = output.filter(_.name == dataKey).head

      // Partition keys are not available in the statistics of the files.
      val partitionKeyFilters = ExpressionSet(Seq(IsNotNull(partitionAttr),
        LessThanOrEqual(partitionAttr, Literal(end, partitionAttr.dataType)),
        GreaterThanOrEqual(partitionAttr, Literal(start, partitionAttr.dataType))))
      logInfo(s"Pruning directories with: ${partitionKeyFilters.mkString(",")}")
      val dataFilters = if (values.nonEmpty)
        Seq(InSet(dataAttr, values.toSet[Any]))
      else Seq.empty[Expression]

      UserDefinedFileSourceStrategy(plan, partitionKeyFilters, dataFilters).head.execute()
    }
  }

}

object UserDefinedFileSourceStrategy extends Logging {
  def apply(plan: LogicalPlan, partitionKeyFilters: ExpressionSet, dataFilters: Seq[Expression]) = plan match {
    case PhysicalOperation(projects, _,
    l@LogicalRelation(fsRelation: HadoopFsRelation, _, table, _)) =>
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

      val outputAttributes = readDataColumns /* ++ partitionColumns*/

      val scan =
        FileSourceScanExec(
          fsRelation,
          outputAttributes,
          outputSchema,
          partitionKeyFilters.toSeq,
          dataFilters,
          table.map(_.identifier))

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