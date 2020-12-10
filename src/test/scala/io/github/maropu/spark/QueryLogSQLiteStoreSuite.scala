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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class QueryLogSQLiteStoreSuite extends QueryTest with SharedSparkSession with BeforeAndAfterEach {

  val queryLogStore = {
    val store = new QueryLogSQLiteStore()
    store.init()
    store
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    queryLogStore.reset()
  }

  test("put/load test") {
    val q1 = QueryLog("2020-12-09 06:27:44.443", "query", 5242,
      Map("a" -> 1, "b" -> 2), Map("execution" -> 5))
    val q2 = QueryLog("2020-12-10 08:00:04.153", "query", 2932,
      Map("b" -> 1), Map("execution" -> 2))
    val q3 = QueryLog("2020-12-13 11:09:51.001", "query", 3921,
      Map("a" -> 1, "b" -> 2, "c" -> 1), Map("execution" -> 3))
    val queryLogs = Seq(q1, q2, q3)
    queryLogs.foreach(queryLogStore.put)
    checkAnswer(queryLogStore.load(), spark.createDataFrame(queryLogs))
  }
}
