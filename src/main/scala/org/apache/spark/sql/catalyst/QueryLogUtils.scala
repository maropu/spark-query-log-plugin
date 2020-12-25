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

package org.apache.spark.sql.catalyst

import java.util.UUID

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}

object QueryLogUtils {

  private val fixedUuid = UUID.fromString("6d37d815-ceea-4ae0-a051-b366f55b0e88")
  // TODO: Use `QueryPlan.transformUpWithNewOutput` instead
  private val fixedExprId = ExprId(0L, fixedUuid)

  def computeFingerprint(plan: LogicalPlan): Int = {
    val p = plan.canonicalized.transform {
      case r @ LocalRelation(output, _, _) =>
        r.copy(output = output.map { a => a.withExprId(fixedExprId) })
      case p => p.transformExpressions {
        case attr: AttributeReference =>
          attr.copy()(fixedExprId, attr.qualifier)
        case alias: Alias =>
          alias.copy()(fixedExprId, alias.qualifier, alias.explicitMetadata)
      }
    }
    // To avoid re-assigning new exprIds (w/ new UUIDs), directly calls `p.hashCode`
    // instead of calling `p.semanticHash()`.
    p.hashCode()
  }

  def computePlanReferences(plan: SparkPlan): Map[String, Int] = {
    val refs = plan.collectLeaves().flatMap {
      case s: FileSourceScanExec if s.tableIdentifier.isDefined =>
        val ident = s.tableIdentifier.get.unquotedString
        s.output.map { a => s"$ident.${a.name}" }
      case p =>
        p.output.map(_.qualifiedName)
    }
    refs.groupBy(identity).map { case (k, refs) => k -> refs.length }
  }
}
