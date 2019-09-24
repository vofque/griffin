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
import org.apache.griffin.measure.step.builder.dsl.expr._
import org.apache.griffin.measure.step.transform.{SparkSqlTransformStep, TransformStep}
import org.apache.griffin.measure.step.write.RecordWriteStep

/**
  * generate validation accuracy dq steps
  */
case class ValidationAccuracyExpr2DQSteps(context: DQContext,
                                          expr: Expr,
                                          ruleParam: RuleParam
                                         ) extends AccuracyExpr2DQSteps(context, expr, ruleParam) {
  import AccuracyExpr2DQSteps._

  override def makeMissRecordsTransStep(sourceName: String): TransformStep = {
    val selClause = s"`${sourceName}`.*"
    val whereClause = expr.coalesceDesc
    val missRecordsSql = s"SELECT ${selClause} FROM `${sourceName}` WHERE NOT(${whereClause})"

    val missRecordsWriteStep = {
      val rwName = ruleParam.getOutputOpt(RecordOutputType).flatMap(_.getNameOpt).getOrElse(missRecordsTableName)
      RecordWriteStep(rwName, missRecordsTableName)
    }

    val missRecordsTransStep =
      SparkSqlTransformStep(missRecordsTableName, missRecordsSql,
        emptyMap, Some(missRecordsWriteStep), true)

    missRecordsTransStep
  }
}
