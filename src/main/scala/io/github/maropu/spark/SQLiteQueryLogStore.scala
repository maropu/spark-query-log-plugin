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

import java.sql.{Connection, DriverManager, Statement}

import scala.util.Random
import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.QueryLogConf._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.ShutdownHookManagerAccessor

private[spark] class SQLiteQueryLogStore extends QueryLogStore with Logging {
  private val sparkHome = sys.env.getOrElse("SPARK_HOME", System.getProperty("java.io.tmpdir"))
  private val dbName = SQLConf.get.queryLogDbName
  private val tableName = SQLConf.get.queryLogTableName
  private val initSql =
    s"""CREATE TABLE IF NOT EXISTS $tableName (
       |  timestamp TEXT,
       |  query TEXT,
       |  fingerprint INTEGER,
       |  refs TEXT,
       |  durationMs TEXT
       |);
     """.stripMargin

  private def dbPath = s"$sparkHome/$dbName.db"
  private def queryLogRandomSamplingRatio = SQLConf.get.queryLogRandomSamplingRatio

  private var connOption: Option[Connection] = None

  private def withJdbcStatement(f: Statement => Unit): Unit = synchronized {
    assert(connOption.nonEmpty, s"Failed to connect database: $dbPath")
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

  override def init(): Unit = {
    try {
      // scalastyle:off println
      println(s"Start logging queries in $dbPath")
      // scalastyle:on println
      Class.forName("org.sqlite.JDBC")
      connOption = Some(DriverManager.getConnection(s"jdbc:sqlite://$dbPath"))
      ShutdownHookManagerAccessor.addShutdownHook { () => connOption.foreach(_.close()) }
      withJdbcStatement { _.execute(initSql) }
    } catch {
      case NonFatal(e) =>
        connOption = None
        throw new IllegalStateException(
          s"Failed to connect database '$dbPath' " +
          s"because ${e.getMessage}")
    }
  }

  override def put(ql: QueryLog): Unit = {
    if (queryLogRandomSamplingRatio > Random.nextDouble()) {
      withJdbcStatement { stmt =>
        val refs = ql.refs.map { case (k, v) => s""""$k": $v""" }.mkString("{", ", ", "}")
        val durationMs = ql.durationMs.map { case (m, t) => s""""$m": $t""" }
          .mkString("{", ", ", "}")
        stmt.execute(s"""
             |INSERT INTO $tableName VALUES (
             |  '${ql.timestamp}', '${ql.query}', ${ql.fingerprint}, '$refs', '$durationMs'
             |);
           """.stripMargin)
      }
    }
  }

  override def load(): DataFrame = SparkSession.getActiveSession.map { sparkSession =>
    sparkSession.read.format("jdbc")
      .option("driver", "org.sqlite.JDBC")
      .option("url", s"jdbc:sqlite:$dbPath")
      .option("dbtable", tableName)
      .load()
  }.getOrElse {
    throw new SparkException("Active Spark session not found")
  }

  override def reset(): Unit = {
    connOption.foreach { conn =>
      try {
        var stmt: Statement = null
        try {
          stmt = conn.createStatement()
          stmt.execute(s"DROP TABLE $tableName")
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
