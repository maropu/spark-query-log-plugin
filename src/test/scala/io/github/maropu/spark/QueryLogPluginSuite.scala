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
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class QueryLogPluginSuite extends QueryTest with SharedSparkSession {

  test("Install with a default query log store") {
    try {
      QueryLogPlugin.install()
      (0 until 10).foreach { _ => sql("SELECT 1").count() }
      TestUtils.waitListenerBusUntilEmpty(spark.sparkContext)
      val queryLogs = QueryLogPlugin.load().where("fingerprint = -453247703")
      assert(queryLogs.count() === 10)
    } finally {
      QueryLogPlugin.uninstall()
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
}
