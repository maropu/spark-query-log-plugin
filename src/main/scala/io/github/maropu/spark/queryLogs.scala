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

import scala.util.Random

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.QueryLogConf._
import org.apache.spark.sql.catalyst.QueryLogUtils
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.QueryExecutionListener

case class QueryLog(
  timestamp: String, query: String, fingerprint: Int, attrRefs: Map[String, Int],
  durationMs: Map[String, Long]) {

  private def maxQueryStringLength = SQLConf.get.maxQueryStringLength

  private def toMapString[K, V](m: Map[K, V]): String =
    m.map { case (k, v) => s""""$k": $v""" }.mkString("{", ", ", "}")

  private def toQueryString(queryStr: String) = {
    val q = queryStr.toString.replaceAll("\n", "\\\\n").replaceAll("\t", "\\\\t")
    if (maxQueryStringLength > 0 && q.length > maxQueryStringLength) {
      q.substring(0, maxQueryStringLength) + "..."
    } else {
      q
    }
  }

  override def toString(): String = {
    s"""{"timestamp": "$timestamp", "query": "${toQueryString(query)}", """ +
      s""""fingerprint": $fingerprint, "attrRefs": ${toMapString(attrRefs)}, """ +
      s""""durationMs": ${toMapString(durationMs)}}"""
  }
}

object QueryLog extends Logging {

  val schema: StructType = Encoders.product[QueryLog].schema

  private[spark] def logBasedOnLevel(f: => String): Unit = {
    val logLevel = SQLConf.get.debugLogLevel
    logLevel match {
      case "TRACE" => logTrace(f)
      case "DEBUG" => logDebug(f)
      case "INFO" => logInfo(f)
      case "WARN" => logWarning(f)
      case "ERROR" => logError(f)
      case _ => logTrace(f)
    }
  }
}

private[spark] class QueryLogListener(queryLogStore: QueryLogStore)
  extends QueryExecutionListener {

  private def queryLogRandomSamplingRatio = SQLConf.get.queryLogRandomSamplingRatio

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
    if (queryLogRandomSamplingRatio > Random.nextDouble()) {
      val query = qe.optimizedPlan.toString()
      val hashv = SemanticHash.hashValue(qe.optimizedPlan)
      val refs = QueryLogUtils.computePlanReferences(qe.sparkPlan)
      val durationMs = {
        val metricMap = qe.tracker.phases.mapValues { ps => ps.endTimeMs - ps.startTimeMs }
        metricMap + ("execution" -> durationNs / (1000 * 1000))
      }
      val queryLog = QueryLog(currentTimestamp, query, hashv, refs, durationMs)
      QueryLog.logBasedOnLevel(queryLog.toString())
      queryLogStore.put(queryLog)
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
}

object QueryLogPlugin extends Logging {

  @volatile private var queryLogStore: QueryLogStore = _
  @volatile private var queryLogListener: QueryLogListener = _
  @volatile private var installed = false

  // Allows extra regularization rules to be injected into [[Regularizer]] at runtime
  @volatile var extraRegularizationRules: Seq[Rule[LogicalPlan]] = Nil

  private def defaultQueryLogStore = SQLConf.get.queryLogStore match {
    case "MEMORY" => new QueryLogMemoryStore()
    case "SQLITE" => new QueryLogSQLiteStore()
    case s => throw new IllegalStateException(s"Illegal query log store: $s")
  }

  def install(logSink: QueryLogStore): Unit = synchronized {
    if (!installed) {
      SparkSession.getActiveSession.map { sparkSession =>
        queryLogStore = logSink
        queryLogListener = new QueryLogListener(logSink)
        logSink.init()
        sparkSession.sqlContext.listenerManager.register(queryLogListener)
        installed = true
      }.getOrElse {
        throw new SparkException("Active Spark session not found")
      }
    } else {
      throw new IllegalStateException(s"`${classOf[QueryLogListener].getSimpleName}` " +
        "already installed")
    }
  }

  def install(): Unit = {
    install(defaultQueryLogStore)
  }

  def uninstall(): Unit = synchronized {
    if (installed) {
      SparkSession.getActiveSession.map { sparkSession =>
        sparkSession.sqlContext.listenerManager.unregister(queryLogListener)
        queryLogStore.release()
        queryLogStore = null
        queryLogListener = null
        installed = false
      }.getOrElse {
        throw new SparkException("Active Spark session not found")
      }
    } else {
      throw new IllegalStateException(s"`${classOf[QueryLogListener].getSimpleName}` " +
        "not installed")
    }
  }

  // For testing
  private[spark] def resetQueryLogs(): Unit = {
    if (installed) {
      queryLogStore.reset()
     } else {
      throw new IllegalStateException(s"`${classOf[QueryLogListener].getSimpleName}` " +
        "not installed")
    }
  }

  def load(): DataFrame = synchronized {
    if (!installed) {
      throw new IllegalStateException(s"`${classOf[QueryLogListener].getSimpleName}` " +
        "not installed")
    } else {
      val queryLogs = queryLogStore.load()
      if (queryLogs.schema.sql != QueryLog.schema.sql) {
        throw new IllegalStateException(s"Illegal query log schema found: ${queryLogs.schema.sql}")
      }
      queryLogs
    }
  }
}
