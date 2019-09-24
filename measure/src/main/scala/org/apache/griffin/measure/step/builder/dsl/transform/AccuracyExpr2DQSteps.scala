/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
package org.apache.griffin.measure.step.builder.dsl.transform

import org.apache.griffin.measure.configuration.dqdefinition.RuleParam
import org.apache.griffin.measure.configuration.enums._
import org.apache.griffin.measure.context.DQContext
import org.apache.griffin.measure.step.DQStep
import org.apache.griffin.measure.step.builder.ConstantColumns
import org.apache.griffin.measure.step.builder.dsl.expr._
import org.apache.griffin.measure.step.transform.{DataFrameOps, DataFrameOpsTransformStep, SparkSqlTransformStep, TransformStep}
import org.apache.griffin.measure.step.transform.DataFrameOps.AccuracyOprKeys
import org.apache.griffin.measure.step.write.{MetricWriteStep, RecordWriteStep}
import org.apache.griffin.measure.utils.ParamUtil._

/**
  * generate accuracy dq steps
  */
private[transform] abstract class AccuracyExpr2DQSteps(context: DQContext,
                                                       expr: Expr,
                                                       ruleParam: RuleParam
                                                      ) extends Expr2DQSteps {
  private object AccuracyKeys {
    val _source = "source"
    val _target = "target"
    val _miss = "miss"
    val _total = "total"
    val _matched = "matched"
    val _matchedFraction = "matchedFraction"
  }
  import AccuracyKeys._
  import AccuracyExpr2DQSteps._

  val details: Map[String, Any] = ruleParam.getDetails
  val accuracyExpr: LogicalExpr = expr.asInstanceOf[LogicalExpr]

  val procType: ProcessType = context.procType
  val timestamp: Long = context.contextId.timestamp

  private val missColName: String = details.getStringOrKey(_miss)
  private val totalColName: String = details.getStringOrKey(_total)
  private val accuracyTableName: String = ruleParam.getOutDfName()
  private val matchedColName: String = details.getStringOrKey(_matched)
  private val matchedFractionColName: String = details.getStringOrKey(_matchedFraction)

  def makeMissRecordsTransStep(sourceName: String): TransformStep

  def makeMissCountTransStep(missRecordsTransStep: TransformStep): TransformStep = {
    val missCountSql = procType match {
      case BatchProcessType =>
        s"SELECT COUNT(*) AS `${missColName}` FROM `${missRecordsTableName}`"
      case StreamingProcessType =>
        s"SELECT `${ConstantColumns.tmst}`,COUNT(*) AS `${missColName}` " +
          s"FROM `${missRecordsTableName}` GROUP BY `${ConstantColumns.tmst}`"
    }

    val missCountTransStep = SparkSqlTransformStep(missCountTableName, missCountSql, emptyMap)
    missCountTransStep.parentSteps += missRecordsTransStep

    missCountTransStep
  }

  def makeTotalCountTransStep(sourceName: String): TransformStep = {
    val totalCountSql = procType match {
      case BatchProcessType => s"SELECT COUNT(*) AS `${totalColName}` FROM `${sourceName}`"
      case StreamingProcessType =>
        s"SELECT `${ConstantColumns.tmst}`, COUNT(*) AS `${totalColName}` " +
          s"FROM `${sourceName}` GROUP BY `${ConstantColumns.tmst}`"
    }

    val totalCountTransStep = SparkSqlTransformStep(totalCountTableName, totalCountSql, emptyMap)

    totalCountTransStep
  }

  def makeAccuracyTransStep(missCountTransStep: TransformStep, totalCountTransStep: TransformStep): TransformStep = {
    val accuracyMetricSql = procType match {
      case BatchProcessType =>
        s"""
             SELECT A.total AS `${totalColName}`,
                    A.miss AS `${missColName}`,
                    (A.total - A.miss) AS `${matchedColName}`,
                    coalesce( (A.total - A.miss) / A.total, 1.0) AS `${matchedFractionColName}`
             FROM (
               SELECT `${totalCountTableName}`.`${totalColName}` AS total,
                      coalesce(`${missCountTableName}`.`${missColName}`, 0) AS miss
               FROM `${totalCountTableName}` LEFT JOIN `${missCountTableName}`
             ) AS A
         """
      case StreamingProcessType =>
        s"""
           |SELECT `${totalCountTableName}`.`${ConstantColumns.tmst}` AS `${ConstantColumns.tmst}`,
           |`${totalCountTableName}`.`${totalColName}` AS `${totalColName}`,
           |coalesce(`${missCountTableName}`.`${missColName}`, 0) AS `${missColName}`,
           |(`${totalCountTableName}`.`${totalColName}` - coalesce(`${missCountTableName}`.`${missColName}`, 0)) AS `${matchedColName}`
           |FROM `${totalCountTableName}` LEFT JOIN `${missCountTableName}`
           |ON `${totalCountTableName}`.`${ConstantColumns.tmst}` = `${missCountTableName}`.`${ConstantColumns.tmst}`
         """.stripMargin
    }

    val accuracyMetricWriteStep = procType match {
      case BatchProcessType =>
        val metricOpt = ruleParam.getOutputOpt(MetricOutputType)
        val mwName = metricOpt.flatMap(_.getNameOpt).getOrElse(ruleParam.getOutDfName())
        val flattenType = metricOpt.map(_.getFlatten).getOrElse(FlattenType.default)
        Some(MetricWriteStep(mwName, accuracyTableName, flattenType))
      case StreamingProcessType => None
    }

    val accuracyTransStep =
      SparkSqlTransformStep(accuracyTableName, accuracyMetricSql, emptyMap, accuracyMetricWriteStep)
    accuracyTransStep.parentSteps += missCountTransStep
    accuracyTransStep.parentSteps += totalCountTransStep

    accuracyTransStep
  }

  def makeAccuracyRecordTransSteps(accuracyTransStep: TransformStep): Seq[TransformStep] = {
    procType match {
      case BatchProcessType => accuracyTransStep :: Nil
      // streaming extra steps
      case StreamingProcessType =>

        // 5. accuracy metric merge
        val accuracyMetricTableName = "__accuracy"
        val accuracyMetricRule = DataFrameOps._accuracy
        val accuracyMetricDetails = Map[String, Any](
          (AccuracyOprKeys._miss -> missColName),
          (AccuracyOprKeys._total -> totalColName),
          (AccuracyOprKeys._matched -> matchedColName)
        )
        val accuracyMetricWriteStep = {
          val metricOpt = ruleParam.getOutputOpt(MetricOutputType)
          val mwName = metricOpt.flatMap(_.getNameOpt).getOrElse(ruleParam.getOutDfName())
          val flattenType = metricOpt.map(_.getFlatten).getOrElse(FlattenType.default)
          MetricWriteStep(mwName, accuracyMetricTableName, flattenType)
        }
        val accuracyMetricTransStep = DataFrameOpsTransformStep(accuracyMetricTableName,
          accuracyTableName, accuracyMetricRule, accuracyMetricDetails, Some(accuracyMetricWriteStep))
        accuracyMetricTransStep.parentSteps += accuracyTransStep

        // 6. collect accuracy records
        val accuracyRecordTableName = "__accuracyRecords"
        val accuracyRecordSql = {
          s"""
             |SELECT `${ConstantColumns.tmst}`, `${ConstantColumns.empty}`
             |FROM `${accuracyMetricTableName}` WHERE `${ConstantColumns.record}`
             """.stripMargin
        }

        val accuracyRecordWriteStep = {
          val rwName =
            ruleParam.getOutputOpt(RecordOutputType).flatMap(_.getNameOpt)
              .getOrElse(missRecordsTableName)

          RecordWriteStep(rwName, missRecordsTableName, Some(accuracyRecordTableName))
        }
        val accuracyRecordTransStep = SparkSqlTransformStep(
          accuracyRecordTableName, accuracyRecordSql, emptyMap, Some(accuracyRecordWriteStep))
        accuracyRecordTransStep.parentSteps += accuracyMetricTransStep

        accuracyRecordTransStep :: Nil
    }
  }

  def getDQSteps(): Seq[DQStep] = {

    val sourceName = details.getString(_source, context.getDataSourceName(0))

    if (!context.runTimeTableRegister.existsTable(sourceName)) {
      warn(s"[${timestamp}] data source ${sourceName} not exists")
      Nil
    } else {

      // 1. miss record
      val missRecordsTransStep = makeMissRecordsTransStep(sourceName)

      // 2. miss count
      val missCountTransStep = makeMissCountTransStep(missRecordsTransStep)

      // 3. total count
      val totalCountTransStep = makeTotalCountTransStep(sourceName)

      // 4. accuracy metric
      val accuracyTransStep = makeAccuracyTransStep(missCountTransStep, totalCountTransStep)

      makeAccuracyRecordTransSteps(accuracyTransStep)
    }
  }

}

private[transform] object AccuracyExpr2DQSteps {

  val missRecordsTableName: String = "__missRecords"
  val missCountTableName: String = "__missCount"
  val totalCountTableName: String = "__totalCount"
}
