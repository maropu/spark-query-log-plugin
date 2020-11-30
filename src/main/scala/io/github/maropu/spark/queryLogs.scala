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

import io.github.maropu.spark.utils.QueryLogStore

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.QueryLogConf._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.QueryExecutionListener

case class QueryLogListener() extends QueryExecutionListener {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    QueryLogStore.put(qe, durationNs)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
}

object QueryLogPlugin {
  private val sparkHome = sys.env.getOrElse("SPARK_HOME", System.getProperty("java.io.tmpdir"))

  def dbName: String = SQLConf.get.queryLogDbName
  def dbPath: String = s"$sparkHome/$dbName.db"
  def tableName: String = SQLConf.get.queryLogTableName

  def install(): Unit = SparkSession.getActiveSession.map { sparkSession =>
    sparkSession.listenerManager.register(new QueryLogListener())
  }.getOrElse {
    throw new SparkException("Active Spark session not found")
  }

  def load(): DataFrame = SparkSession.getActiveSession.map { sparkSession =>
    sparkSession.read.format("jdbc")
      .option("driver", "org.sqlite.JDBC")
      .option("url", s"jdbc:sqlite:$dbPath")
      .option("dbtable", tableName)
      .load()
  }.getOrElse {
    throw new SparkException("Active Spark session not found")
  }
}
