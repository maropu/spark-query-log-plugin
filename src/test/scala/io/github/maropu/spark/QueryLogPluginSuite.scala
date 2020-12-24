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

package io.github.maropu.spark

import scala.collection.mutable

import org.apache.spark.TestUtils
import org.apache.spark.sql.{QueryLogConf, QueryTest}
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

class QueryLogPluginSuite extends QueryTest with SharedSparkSession {

  private def withQueryStore(f: => Unit): Unit = {
    QueryLogPlugin.install()
    try { f } finally {
      QueryLogPlugin.resetQueryLogs()
      QueryLogPlugin.uninstall()
    }
  }

  test("Install with a default query log store") {
    withQueryStore {
      (0 until 10).foreach { _ => sql("SELECT 1").count() }
      TestUtils.waitListenerBusUntilEmpty(spark.sparkContext)
      val queryLogs = QueryLogPlugin.load().where("fingerprint = -453247703")
      assert(queryLogs.count() === 10)
    }
  }

  test("Install with a user-defined query log store") {
    try {
      val queryLogs = mutable.ArrayBuffer[QueryLog]()
      QueryLogPlugin.install((queryLog: QueryLog) => queryLogs.append(queryLog))
      (0 until 10).foreach { _ => sql("SELECT 1").count() }
      TestUtils.waitListenerBusUntilEmpty(spark.sparkContext)
      assert(queryLogs.length === 10)
      assert(queryLogs.map(_.fingerprint).distinct.length === 1)
    } finally {
      QueryLogPlugin.uninstall()
    }
  }

  test("Not installed exception") {
    val errMsg = intercept[IllegalStateException] {
      QueryLogPlugin.uninstall()
    }.getMessage
    assert(errMsg === "`QueryLogListener` not installed")
  }

  test("Excludes existing regularization rules") {
    withQueryStore {
      sql("SELECT 1").collect()
      sql("SELECT v FROM VALUES (1) t(v)").collect()
      TestUtils.waitListenerBusUntilEmpty(spark.sparkContext)
      // Assumes that the fingerprint `-447001758` represents "SELECT 1" and [[RegularizeOneRow]]
      // converts "SELECT v FROM VALUES (1) t(v)" into "SELECT 1".
      val ql = QueryLogPlugin.load().where("fingerprint = -447001758")
      assert(ql.count() === 2)

      withSQLConf(QueryLogConf.QUERY_LOG_REGULARIZER_EXCLUDED_RULES.key ->
          "io.github.maropu.spark.regularizer.RegularizeOneRow") {
        sql("SELECT v FROM VALUES (1) t(v)").collect()
        TestUtils.waitListenerBusUntilEmpty(spark.sparkContext)
        assert(ql.count() === 2)
      }
    }
  }

  test("Inject extra regularization rules") {
    withQueryStore {
      sql("SELECT 1").collect()
      sql("SELECT 2").collect()
      TestUtils.waitListenerBusUntilEmpty(spark.sparkContext)
      val ql = QueryLogPlugin.load().where("fingerprint = -447001758 OR fingerprint = -1797021872")
      assert(ql.selectExpr("fingerprint").distinct().count() === 2)

      QueryLogPlugin.resetQueryLogs()
      assert(ql.count() === 0)

      QueryLogPlugin.extraRegularizationRules = Seq(new Rule[LogicalPlan] {
        override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
          case p @ Project(Seq(a @ Alias(Literal(v, dt), name)), _: OneRowRelation)
              if dt == IntegerType && v.asInstanceOf[Int] == 1 =>
            p.copy(projectList = Alias(Literal(2, dt), name)() :: Nil)
        }
      })
      try {
        sql("SELECT 1").collect()
        sql("SELECT 2").collect()
        TestUtils.waitListenerBusUntilEmpty(spark.sparkContext)
        assert(ql.selectExpr("fingerprint").distinct().count() === 1)
      } finally {
        QueryLogPlugin.extraRegularizationRules = Nil
      }
    }
  }
}
