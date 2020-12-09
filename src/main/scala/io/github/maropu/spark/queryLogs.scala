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

import java.util.TimeZone

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.QueryLogConf._
import org.apache.spark.sql.catalyst.QueryLogUtils
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.QueryExecutionListener

case class QueryLog(
  timestamp: String, query: String, fingerprint: Int, refs: Map[String, Int],
  durationMs: Map[String, Long])

private[spark] class QueryLogListener(queryLogStore: QueryLogStore)
  extends QueryExecutionListener {

  private def timestampFormatter = {
    val timeZoneId = DateTimeUtils.getZoneId(SparkSession.getActiveSession.map { sparkSession =>
      sparkSession.sessionState.conf.sessionLocalTimeZone
    }.getOrElse {
      TimeZone.getDefault.getID
    })
    TimestampFormatter.getFractionFormatter(timeZoneId)
  }

  private def currentTimestamp: String = {
    timestampFormatter.format(System.currentTimeMillis() * 1000L)
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    val query = qe.optimizedPlan.toString()
    val fingerprint = QueryLogUtils.computeFingerprint(qe)
    val refs = QueryLogUtils.computePlanReferences(qe)
    val durationMs = {
      val metricMap = qe.tracker.phases.mapValues { ps => ps.endTimeMs - ps.startTimeMs }
      metricMap + ("execution" -> durationNs / (1000 * 1000))
    }
    val queryLog = QueryLog(currentTimestamp, query, fingerprint, refs, durationMs)
    queryLogStore.put(queryLog)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
}

object QueryLogPlugin extends Logging {

  private val queryLogStore = SQLConf.get.queryLogStore match {
    case "MEMORY" => new QueryLogMemoryStore()
    case "SQLITE" => new QueryLogSQLiteStore()
    case s => throw new IllegalStateException(s"Illegal query log store: $s")
  }

  private var initialized = false

  def install(): Unit = if (!initialized) {
    SparkSession.getActiveSession.map { sparkSession =>
      queryLogStore.init()
      sparkSession.listenerManager.register(new QueryLogListener(queryLogStore))
      initialized = true
    }.getOrElse {
      throw new SparkException("Active Spark session not found")
    }
  } else {
    logWarning(s"`${classOf[QueryLogListener].getSimpleName}` already installed")
  }

  def load(): DataFrame = queryLogStore.load()
}
