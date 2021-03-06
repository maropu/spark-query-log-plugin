/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.maropu.spark.regularizer

import io.github.maropu.spark.QueryLogPlugin

import org.apache.spark.sql.QueryLogConf
import org.apache.spark.sql.QueryLogConf._
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, Least, Literal}
import org.apache.spark.sql.catalyst.optimizer.CollapseProject
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.internal.SQLConf

/**
 * Standardizes a given logical plan into a more regular form with techniques such as
 * an OR-UNION transformation. This process of regularization aims to produce
 * a new query that is more likely to be structurally similar to other semantically
 * similar queries. For more details, see a paper below:
 *
 *  - Gokhan Kul et al., "Similarity Metrics for SQL Query Clustering",
 *    IEEE Transactions on Knowledge and Data Engineering, vol.30, no.12, pp.2408-2420, 2018.
 */
object Regularizer extends RuleExecutor[LogicalPlan] {

  protected def fixedPoint =
    FixedPoint(
      SQLConf.get.regularizerMaxIterations,
      maxIterationsSetting = QueryLogConf.QUERY_LOG_REGULARIZER_MAX_ITERATIONS.key)

  private def defaultBatches: Seq[Batch] =
    Batch("Regularization", Once,
      // TODO: Adds a rule to regularize a join order
      RegularizeOneRow,
      EliminateLimits,
      CollapseProject
    ) ::
    Batch("User Provided Regularization Rules", fixedPoint,
      QueryLogPlugin.extraRegularizationRules: _*
    ) :: Nil

  private def stringToSeq(str: String) =
    str.split(",").map(_.trim()).filter(_.nonEmpty)

  /**
   * Returns (defaultBatches - excludedRules), the rule batches that
   * eventually run in [[Regularizer]].
   */
  final override def batches: Seq[Batch] = {
    val excludedRules = SQLConf.get.regularizerExcludedRules.toSeq.flatMap(stringToSeq)
    if (excludedRules.isEmpty) {
      defaultBatches
    } else {
      defaultBatches.flatMap { batch =>
        val filteredRules = batch.rules.filter { rule =>
          val exclude = excludedRules.contains(rule.ruleName)
          if (exclude) {
            logInfo(s"Regularization rule '${rule.ruleName}' is excluded from the regularizer.")
          }
          !exclude
        }
        if (batch.rules == filteredRules) {
          Some(batch)
        } else if (filteredRules.nonEmpty) {
          Some(Batch(batch.name, batch.strategy, filteredRules: _*))
        } else {
          logInfo(s"Regularization batch '${batch.name}' is excluded from the regularizer " +
            s"as all enclosed rules have been excluded.")
          None
        }
      }
    }
  }
}

object RegularizeOneRow extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case LocalRelation(output, data, false) if data.length == 1 =>
      val dataTypes = output.map(_.dataType)
      val projectList = data.head.toSeq(dataTypes).zip(output).map { case (cell, attr) =>
        Alias(Literal(cell, attr.dataType), attr.name)(exprId = attr.exprId)
      }
      Project(projectList, OneRowRelation())
  }
}

/**
 * This rule optimizes Limit operators by:
 * 1. Eliminate [[Limit]] operators if it's child max row <= limit.
 * 2. Combines two adjacent [[Limit]] operators into one, merging the
 *    expressions into one single expression.
 *
 * TODO: This rule will be removed from [[Regularizer]] when landing on Spark v3.1.0
 * because the version implements the rule by default.
 */
object EliminateLimits extends Rule[LogicalPlan] {
  private def canEliminate(limitExpr: Expression, child: LogicalPlan): Boolean = {
    limitExpr.foldable && child.maxRows.exists { _ <= limitExpr.eval().asInstanceOf[Int] }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case Limit(l, child) if canEliminate(l, child) =>
      child

    case GlobalLimit(le, GlobalLimit(ne, grandChild)) =>
      GlobalLimit(Least(Seq(ne, le)), grandChild)
    case LocalLimit(le, LocalLimit(ne, grandChild)) =>
      LocalLimit(Least(Seq(ne, le)), grandChild)
    case Limit(le, Limit(ne, grandChild)) =>
      Limit(Least(Seq(ne, le)), grandChild)
  }
}
