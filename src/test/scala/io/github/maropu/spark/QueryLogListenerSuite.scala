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
import org.apache.spark.sql.{QueryLogConf, QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession

class QueryLogListenerSuite
  extends QueryTest
  with SharedSparkSession
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  val (queryLogStore, queryLogListener) = {
    val store = new QueryLogMemoryStore()
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
    TestUtils.waitListenerBusUntilEmpty(spark.sparkContext)
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
    assert(results.toSet === Set(Row(-447001758), Row(474424051), Row(652410493)))
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
    assert(results === Seq(Row(91016659)))
  }

  test("refs test") {
    withTable("t1") {
      sql("CREATE TABLE t1 (a INT, b INT, c INT, d INT) using parquet")

      def checkReferences(query: String, expectedRefs: Map[String, Int]): Unit = {
        sql(query).collect()
        TestUtils.waitListenerBusUntilEmpty(spark.sparkContext)
        val refs = queryLogStore.load()
          .where("arrays_overlap(map_keys(refs), array('a', 'b', 'c', 'd'))")
          .selectExpr("refs")
          .collect()
        assert(refs === Seq(Row(expectedRefs)))
        queryLogStore.reset()
      }

      checkReferences(
        s"""
           |SELECT c, d FROM t1 WHERE c = 1 AND d = 2
         """.stripMargin,
        Map("c" -> 1, "d" -> 1))

      checkReferences(
        s"""
           |SELECT lt.d, lt.c, lt.d FROM t1 lt, t1 rt
           |WHERE lt.a = rt.a AND lt.b = rt.b AND lt.d = rt.d
         """.stripMargin,
        Map("a" -> 2, "b" -> 2, "c" -> 1, "d" -> 2))

      checkReferences(
        s"""
           |SELECT d, SUM(c) FROM t1 GROUP BY d HAVING SUM(c) > 10
         """.stripMargin,
        Map("c" -> 1, "d" -> 1))
    }
  }

  test("random sampling") {
    withSQLConf(QueryLogConf.QUERY_LOG_RANDOM_SAMPLING_RATIO.key -> "0.05") {
      (0 until 10).foreach { _ => sql("SELECT 10").count() }
      TestUtils.waitListenerBusUntilEmpty(spark.sparkContext)
      assert(queryLogStore.load().count() < 4)
      queryLogStore.reset()
    }
    withSQLConf(QueryLogConf.QUERY_LOG_RANDOM_SAMPLING_RATIO.key -> "1.0") {
      (0 until 10).foreach { _ => sql("SELECT 10").count() }
      TestUtils.waitListenerBusUntilEmpty(spark.sparkContext)
      assert(queryLogStore.load().count() >= 10)
    }
  }
}
