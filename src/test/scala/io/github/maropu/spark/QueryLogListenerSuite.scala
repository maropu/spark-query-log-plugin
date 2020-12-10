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

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.spark.TestUtils
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession

class QueryLogListenerSuite
  extends QueryTest
  with SharedSparkSession
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  val (queryLogStore, queryLogListener) = {
    val store = new QueryLogSQLiteStore()
    store.init()
    val listener = new QueryLogListener(store)
    (store, listener)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.listenerManager.register(queryLogListener)
  }

  override def afterAll(): Unit = {
    try {
      spark.listenerManager.unregister(queryLogListener)
    } finally {
      super.afterAll()
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    queryLogStore.reset()
  }

  test("registers a listener to store query logs") {
    sql("SELECT 1").collect()
    sql("SELECT k, SUM(v) FROM VALUES (1, 1) t(k, v) GROUP BY k").collect()
    sql("SELECT version()").collect()
    TestUtils.waitListenerBusUntilEmpty(spark.sparkContext)
    val results = queryLogStore.load()
      .where("query LIKE '%Project%' OR query LIKE '%Aggregate%'")
      .selectExpr("fingerprint")
      .collect()
    assert(results.toSet === Set(Row(-447001758), Row(1120964768), Row(652410493)))
  }

  test("fingerprint test") {
    sql(s"""
         |SELECT COUNT(v) AS v, k AS k
         |FROM VALUES (1, 1) t(k, v)
         |GROUP BY 2
       """.stripMargin).count()
    sql(s"""
         |SELECT a AS key, SUM(b) AS value
         |FROM (
         |  SELECT * FROM VALUES (1, 1) s(a, b)
         |)
         |GROUP BY a
       """.stripMargin).count()
    TestUtils.waitListenerBusUntilEmpty(spark.sparkContext)
    val results = queryLogStore.load()
      .where("query LIKE '%Aggregate%'")
      .selectExpr("fingerprint")
      .distinct()
      .collect()
    assert(results === Seq(Row(2090927494)))
  }

  test("refs test") {
    withTable("t") {
      sql("CREATE TABLE t (a INT, b INT, c INT, d INT) using parquet")
      sql(s"""
           |SELECT c, d FROM t WHERE c = 1 AND d = 2
         """.stripMargin).count()
      sql(s"""
           |SELECT lt.d, lt.c, lt.d FROM t lt, t rt
           |WHERE lt.a = rt.a AND lt.b = rt.b AND lt.d = rt.d
         """.stripMargin).count()
      sql(s"""
           |SELECT d, SUM(c) FROM t GROUP BY d HAVING SUM(c) > 10
         """.stripMargin).count()
    }
    TestUtils.waitListenerBusUntilEmpty(spark.sparkContext)
    val results = queryLogStore.load()
      .where("arrays_overlap(map_keys(refs), array('a', 'b', 'c', 'd'))")
      .selectExpr("refs")
      .collect()
    assert(results.toSet === Set(
      Row(Map("a" -> 2, "b" -> 2, "d" -> 2)),
      Row(Map("c" -> 1, "d" -> 1))
    ))
  }
}
