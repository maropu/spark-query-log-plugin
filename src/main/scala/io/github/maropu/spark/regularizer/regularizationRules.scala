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

import java.util.UUID

import io.github.maropu.spark.QueryLogPlugin

import org.apache.spark.sql.QueryLogConf
import org.apache.spark.sql.QueryLogConf._
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExprId, Literal}
import org.apache.spark.sql.catalyst.optimizer.CollapseProject
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.execution.command.CreateDataSourceTableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
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
private[spark] object Regularizer extends RuleExecutor[LogicalPlan] {

  protected def fixedPoint =
    FixedPoint(
      SQLConf.get.regularizerMaxIterations,
      maxIterationsSetting = QueryLogConf.QUERY_LOG_REGULARIZER_MAX_ITERATIONS.key)

  private def defaultBatches: Seq[Batch] =
    Batch("Regularization", FixedPoint(1),
      // TODO: Adds a rule to regularize a join order
      RegularizeExprs,
      RegularizeOneRow,
      RegularizeExprOrders,
      RegularizeCatalogInfo,
      CollapseProject
    ) ::
    Batch("User Provided Regularization Rules", fixedPoint,
      QueryLogPlugin.extraRegularizationRules: _*
    ) :: Nil

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

  object RegularizeExprOrders extends Rule[LogicalPlan] {

    override def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
      case p @ Project(projList, _) =>
        val newProjList = projList.sortBy(_.hashCode)
        p.copy(projectList = newProjList)

      case Aggregate(groupingExprs, aggExprs, child) =>
        val newGroupingExprs = groupingExprs.sortBy(_.hashCode)
        val newAggExprs = aggExprs.sortBy(_.hashCode)
        Aggregate(newGroupingExprs, newAggExprs, child)
    }
  }

  object RegularizeExprs extends Rule[LogicalPlan] {

    val fixedExprId = ExprId(0L, UUID.fromString("6d37d815-ceea-4ae0-a051-b366f55b0e88"))

    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transform {
        case p @ Project(projList, _) =>
          val newProjList = projList.map {
            case a: Alias => a
            case ne => Alias(ne, "none")(exprId = fixedExprId)
          }
          p.copy(projectList = newProjList)

        case a @ Aggregate(_, aggExprs, _) =>
          val newAggExprs = aggExprs.map {
            case a: Alias => a
            case ne => Alias(ne, "none")(exprId = fixedExprId)
          }
          a.copy(aggregateExpressions = newAggExprs)

        case r @ LocalRelation(output, _, _) =>
          r.copy(output = output.map(_.withExprId(fixedExprId)))

        case r @ HiveTableRelation(_, dataCols, partCols, _, _) =>
          def clearExprs(o: Seq[AttributeReference]) = o.map(_.withExprId(fixedExprId))
          r.copy(dataCols = clearExprs(dataCols), partitionCols = clearExprs(partCols))
      }.transformAllExpressions {
        case attr: AttributeReference =>
          attr.copy()(fixedExprId, attr.qualifier)

        case a @ Alias(child, _) =>
          Alias(child, "none")(fixedExprId, a.qualifier, a.explicitMetadata)
      }
    }
  }

  object RegularizeCatalogInfo extends Rule[LogicalPlan] {

    override def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
      case r @ LogicalRelation(_, _, table, _) =>
        // TODO: Reconsiders this
        val newCatalogTable = table.map(_.copy(createTime = 0L))
        r.copy(relation = null, output = Nil, catalogTable = newCatalogTable)
        new LogicalRelation(null, Nil, newCatalogTable, false)

      case r @ HiveTableRelation(table, _, _, _, _) =>
        // TODO: Reconsiders this
        val newCatalogTable = Some(table.copy(createTime = 0L))
        new LogicalRelation(null, Nil, newCatalogTable, false)

      case _ @ CreateDataSourceTableCommand(table, ignoreIfExists) =>
        CreateDataSourceTableCommand(table.copy(createTime = 0L), ignoreIfExists)
    }
  }

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
          // TODO: Remove this logic
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
