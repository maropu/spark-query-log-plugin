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

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.BooleanType

private[spark] object StructuralHasher {

  private val emptyLocalRelation = LocalRelation(Nil, Nil, false)
  private val emptyLogicalRelation = LogicalRelation(null, Nil, None, false)
  private val fakePredicate = Literal(true, BooleanType)

  // Only keep the structure of a plan tree
  private def normalizePlan(p: LogicalPlan): LogicalPlan = p.transform {
    // TODO: Normalize the other logical plans
    case Project(_, child) => Project(Nil, child)
    case Aggregate(_, _, child) => Aggregate(Nil, Nil, child)
    case Join(left, right, joinType, _, _) => Join(left, right, joinType, None, JoinHint.NONE)
    case Filter(_, child) => Filter(fakePredicate, child)
    case _: LocalRelation => emptyLocalRelation
    case _: LogicalRelation => emptyLogicalRelation
  }

  def computeHashValue(p: LogicalPlan): Int = {
    normalizePlan(p).semanticHash()
  }
}
