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

package io.github.maropu.spark.utils

import java.sql.{Connection, DriverManager, Statement}
import java.util.TimeZone

import scala.util.Random
import scala.util.control.NonFatal

import io.github.maropu.spark.QueryLogPlugin

import org.apache.spark.internal.Logging
import org.apache.spark.sql.QueryLogConf._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.QueryLogUtils
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.ShutdownHookManagerAccessor

object QueryLogStore extends Logging {
  private val initSql =
    s"""CREATE TABLE IF NOT EXISTS ${QueryLogPlugin.tableName} (
       |  timestamp TEXT,
       |  query TEXT,
       |  fingerprint INTEGER,
       |  refs TEXT,
       |  durationMs TEXT
       |);
     """.stripMargin

  private def queryLogRandomSamplingRatio = SQLConf.get.queryLogRandomSamplingRatio

  private var connOption: Option[Connection] = None

  private def withJdbcStatement(f: Statement => Unit): Unit = synchronized {
    assert(connOption.nonEmpty, s"Failed to connect database: ${QueryLogPlugin.dbPath}")
    connOption.foreach { conn =>
      var stmt: Statement = null
      try {
        stmt = conn.createStatement()
        f(stmt)
      } finally {
        if (stmt != null) {
          stmt.close()
        }
      }
    }
  }

  synchronized {
    try {
      // scalastyle:off println
      println(s"Start logging queries in ${QueryLogPlugin.dbPath}...")
      // scalastyle:on println
      Class.forName("org.sqlite.JDBC")
      connOption = Some(DriverManager.getConnection(s"jdbc:sqlite://${QueryLogPlugin.dbPath}"))
      ShutdownHookManagerAccessor.addShutdownHook { () => connOption.foreach(_.close()) }
      withJdbcStatement { _.execute(initSql) }
    } catch {
      case NonFatal(e) =>
        connOption = None
        throw new IllegalStateException(
          s"Failed to connect database '${QueryLogPlugin.dbPath}' " +
          s"because ${e.getMessage}")
    }
  }

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

  private def computeFootprint(qe: QueryExecution): Int = {
    QueryLogUtils.computeFootPrint(qe)
  }

  private def computeReferences(qe: QueryExecution): String = {
    val refs = QueryLogUtils.computePlanReferences(qe).map { case (k, v) => s""""$k": $v""" }
    s"""{${refs.mkString(", ")}}"""
  }

  def put(qe: QueryExecution, executionNs: Long): Unit = {
    if (queryLogRandomSamplingRatio > Random.nextDouble()) {
      withJdbcStatement { stmt =>
        val durationMs = {
          val metricSeq = qe.tracker.phases.mapValues { ps => ps.endTimeMs - ps.startTimeMs }.toSeq
          val metrics = metricSeq :+ ("execution" -> executionNs / (1000 * 1000))
          metrics.map { case (m, t) => s""""$m": $t""" }.mkString("{", ", ", "}")
        }
        stmt.execute(s"""
             |INSERT INTO ${QueryLogPlugin.tableName} VALUES (
             |  '$currentTimestamp', '${qe.optimizedPlan}', ${computeFootprint(qe)},
             |  '${computeReferences(qe)}', '$durationMs'
             |);
           """.stripMargin)
      }
    }
  }

  def reset(): Unit = {
    connOption.foreach { conn =>
      try {
        var stmt: Statement = null
        try {
          stmt = conn.createStatement()
          stmt.execute(s"DROP TABLE ${QueryLogPlugin.tableName}")
          stmt.execute(initSql)
        } finally {
          if (stmt != null) {
            stmt.close()
          }
        }
      } catch {
        case NonFatal(_) =>
          connOption = None
      }
    }
  }
}
