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
    assert(results.toSet === Set(Row(-447001758), Row(-1806854913), Row(-53078370)))
  }

  test("fingerprint simple test") {
    sql(s"""
         |SELECT COUNT(v) AS v, k AS k
         |FROM VALUES (1, 1) t(k, v)
         |GROUP BY 2
       """.stripMargin).collect()
    sql(s"""
         |SELECT a AS key, SUM(b) AS value
         |FROM (
         |  SELECT * FROM VALUES (1, 1) s(a, b)
         |)
         |GROUP BY a
       """.stripMargin).collect()
    TestUtils.waitListenerBusUntilEmpty(spark.sparkContext)
    val results = queryLogStore.load()
      .where("query LIKE '%Aggregate%'")
      .selectExpr("fingerprint")
      .distinct()
      .collect()
    assert(results === Seq(Row(2140489373), Row(-687168322)))
  }

  ignore("fingerprint test - multiple join") {
     withTable("ft", "dt1", "dt2", "dt3") {
       sql("CREATE TABLE ft (a INT) USING parquet")
       sql("CREATE TABLE dt1 (b INT) USING parquet")
       sql("CREATE TABLE dt2 (c INT) USING parquet")
       sql("CREATE TABLE dt3 (d INT) USING parquet")

       sql(s"""
            |SELECT * FROM ft, dt1, dt2, dt3
            |WHERE ft.a = dt1.b AND ft.a = dt2.c AND ft.a = dt3.d
          """.stripMargin).collect()
       sql(s"""
            |SELECT * FROM ft, dt1, dt2, dt3
            |WHERE ft.a = dt3.d AND ft.a = dt2.c AND ft.a = dt1.b
          """.stripMargin).collect()
       sql(s"""
            |SELECT * FROM ft, dt1, dt2, dt3
            |WHERE ft.a = dt2.c AND ft.a = dt3.d AND ft.a = dt1.b
          """.stripMargin).collect()

       // TODO: Adds a rule to regularize a join order
       // The current fingerprint of a join query below is `-934549390`
       sql(s"""
            |SELECT * FROM (
            |  SELECT a, c FROM ft, dt2 WHERE ft.a = dt2.c
            |) t, dt1, dt3
            |WHERE t.a = dt1.b AND t.a = dt3.d
          """.stripMargin).collect()

       TestUtils.waitListenerBusUntilEmpty(spark.sparkContext)
       val ql = queryLogStore.load()
       ql.show()
       assert(ql.where("fingerprint = -1382635878").count() === 3)
       assert(ql.where("fingerprint = 755726794").count() === 1)
     }
  }

  test("attrRefs test") {
    withTable("t1", "t2") {
      sql("CREATE TABLE t1 (a INT, b INT, c INT, d INT) USING parquet")
      sql("CREATE TABLE t2 (e INT, f INT, g INT) USING parquet")

      def checkAttrReferences(query: String, expectedRefs: Map[String, Int]): Unit = {
        sql(query).collect()
        TestUtils.waitListenerBusUntilEmpty(spark.sparkContext)
        val attrs = Seq("t1.a", "t1.b", "t1.c", "t1.d", "t2.e", "t2.f", "t2.g")
        val refs = queryLogStore.load()
          .where("arrays_overlap(map_keys(attrRefs), " +
            s"array(${attrs.map { a => s"'default.$a'" }.mkString(", ")}))")
          .selectExpr("attrRefs")
          .collect()
        assert(refs === Seq(Row(expectedRefs)))
        queryLogStore.reset()
      }

      checkAttrReferences(
        s"""
           |SELECT a, b FROM t1
         """.stripMargin,
        Map("default.t1.a" -> 1, "default.t1.b" -> 1))

      checkAttrReferences(
        s"""
           |SELECT a, c FROM t1 WHERE b = 1 AND d = 2
         """.stripMargin,
        Map("default.t1.a" -> 1, "default.t1.b" -> 1, "default.t1.c" -> 1, "default.t1.d" -> 1))

      checkAttrReferences(
        s"""
           |SELECT c, d, 1 AS e, 2 AS f FROM t1 WHERE c = 1 AND d = 2
         """.stripMargin,
        Map("default.t1.c" -> 1, "default.t1.d" -> 1))

      checkAttrReferences(
        s"""
           |SELECT * FROM t1 WHERE a > 1 AND a = 5
         """.stripMargin,
        Map("default.t1.a" -> 1, "default.t1.b" -> 1, "default.t1.c" -> 1, "default.t1.d" -> 1))

      checkAttrReferences(
        s"""
           |SELECT lt.d, lt.c, lt.d FROM t1 lt, t1 rt
           |WHERE lt.a = rt.a AND lt.b = rt.b AND lt.d = rt.d
         """.stripMargin,
        Map("default.t1.a" -> 2, "default.t1.b" -> 2, "default.t1.c" -> 1, "default.t1.d" -> 2))

      checkAttrReferences(
        s"""
           |SELECT d, SUM(c) FROM t1 GROUP BY d HAVING SUM(c) > 10
         """.stripMargin,
        Map("default.t1.c" -> 1, "default.t1.d" -> 1))

      checkAttrReferences(
        s"""
           |SELECT SUM(g), COUNT(a) FROM t1, t2 WHERE t1.a = t2.f
           |GROUP BY f, c
         """.stripMargin,
        Map("default.t1.a" -> 1, "default.t1.c" -> 1, "default.t2.f" -> 1, "default.t2.g" -> 1))
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
