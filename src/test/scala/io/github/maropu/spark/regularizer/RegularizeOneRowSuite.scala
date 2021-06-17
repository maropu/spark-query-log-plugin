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

import io.github.maropu.spark.regularizer.RegularizeOneRow

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Alias, GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.IntegerType

class RegularizeOneRowSuite extends PlanTest {

  private object Regularizer extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Batch("Regularization", Once,
        RegularizeOneRow,
      ) :: Nil
  }

  private def computeFingerprint(p: LogicalPlan): Int = {
    p.canonicalized.hashCode
  }

  private def compareFingerprint(p1: LogicalPlan, p2: LogicalPlan) =
    computeFingerprint(p1) === computeFingerprint(p2)

  test("regularize one row") {
    val output = Seq('a.int, 'b.int, 'c.int)
    val row = Array[Any](1, 2, 3)
    val oneRow = new GenericInternalRow(row)
    val p1 = new LocalRelation(output, oneRow :: Nil, isStreaming = false)
    val projectList = row.map { cell => Alias(Literal(cell, IntegerType), "v")() }
    val p2 = Project(projectList, OneRowRelation())
    assert(!compareFingerprint(p1, p2))
    assert(compareFingerprint(Regularizer.execute(p1), p2))
  }
}
