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

package org.apache.spark.sql

import java.util.Locale

import scala.language.implicitConversions

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry, ConfigReader}
import org.apache.spark.sql.internal.SQLConf

object QueryLogConf {

  /**
   * Implicitly injects the [[QueryLogConf]] into [[SQLConf]].
   */
  implicit def SQLConfToQueryLogCOnf(conf: SQLConf): QueryLogConf = new QueryLogConf(conf)

  private val sqlConfEntries = SQLConf.sqlConfEntries

  private def register(entry: ConfigEntry[_]): Unit = sqlConfEntries.synchronized {
    require(!sqlConfEntries.containsKey(entry.key),
      s"Duplicate SQLConfigEntry. ${entry.key} has been registered")
    sqlConfEntries.put(entry.key, entry)
  }

  def buildConf(key: String): ConfigBuilder = ConfigBuilder(key).onCreate(register)

  def buildStaticConf(key: String): ConfigBuilder = {
    ConfigBuilder(key).onCreate { entry =>
      SQLConf.staticConfKeys.add(entry.key)
      register(entry)
    }
  }

  object QueryLogStore extends Enumeration {
    val MEMORY, SQLITE = Value
  }

  val QUERY_LOG_STORE = buildStaticConf("spark.sql.queryLogStore")
    .internal()
    .doc("query log store to use.")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(QueryLogStore.values.map(_.toString))
    .createWithDefault(QueryLogStore.SQLITE.toString)

  val QUERY_LOG_DB_NAME = buildStaticConf("spark.sql.queryLog.dbName")
    .internal()
    .doc("Database name to store query logs.")
    .stringConf
    .createWithDefault("querylogs")

  val QUERY_LOG_TABLE_NAME = buildStaticConf("spark.sql.queryLog.tableName")
    .internal()
    .doc("Table name to store query logs.")
    .stringConf
    .createWithDefault("__spark_query_logs")

  val QUERY_LOG_RANDOM_SAMPLING_RATIO = buildConf("spark.sql.queryLog.randomSamplingRatio")
    .doc("Sampling ratio for query logs.")
    .doubleConf
    .checkValue(v => 0.0 <= v && v <= 1.0, "The value must be in [0.0, 1.0].")
    .createWithDefault(1.0)

  val QUERY_LOG_DEBUG_LOG_LEVEL = buildConf("spark.sql.queryLog.debugLogLevel")
    .internal()
    .doc("Configures the log level for query logging. The value can be 'TRACE', 'DEBUG', 'INFO', " +
      "'WARN', or 'ERROR'. The default log level is 'DEBUG'.")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(Set("TRACE", "DEBUG", "INFO", "WARN", "ERROR"))
    .createWithDefault("DEBUG")

  val QUERY_LOG_MAX_QUERY_STRING_LENGTH = buildConf("spark.sql.queryLog.maxQueryStringLength")
    .internal()
    .doc("The maximum length of a query string in query logs.")
    .intConf
    .createWithDefault(15)

  val QUERY_LOG_REGULARIZER_MAX_ITERATIONS =
    buildConf("spark.sql.queryLog.regularizer.maxIterations")
      .internal()
      .doc("The max number of iterations the regularizer runs.")
      .intConf
      .createWithDefault(100)

  val QUERY_LOG_REGULARIZER_EXCLUDED_RULES =
    buildConf("spark.sql.queryLog.regularizer.excludedRules")
      .doc("Configures a list of rules to be disabled in the regularizer, in which the rules are " +
        "specified by their rule names and separated by comma.")
      .stringConf
      .createOptional
}

class QueryLogConf(conf: SQLConf) {
  import QueryLogConf._

  private val reader = new ConfigReader(conf.settings)

  def queryLogStore: String = getConf(QUERY_LOG_STORE)

  def queryLogDbName: String = getConf(QUERY_LOG_DB_NAME)

  def queryLogTableName: String = getConf(QUERY_LOG_TABLE_NAME)

  def queryLogRandomSamplingRatio: Double = getConf(QUERY_LOG_RANDOM_SAMPLING_RATIO)

  def debugLogLevel: String = getConf(QUERY_LOG_DEBUG_LOG_LEVEL)

  def maxQueryStringLength: Int = getConf(QUERY_LOG_MAX_QUERY_STRING_LENGTH)

  def regularizerMaxIterations: Int = getConf(QUERY_LOG_REGULARIZER_MAX_ITERATIONS)

  def regularizerExcludedRules: Option[String] = getConf(QUERY_LOG_REGULARIZER_EXCLUDED_RULES)

  /**
   * Return the value of configuration property for the given key. If the key is not set yet,
   * return `defaultValue` in [[ConfigEntry]].
   */
  private def getConf[T](entry: ConfigEntry[T]): T = {
    require(sqlConfEntries.get(entry.key) == entry || SQLConf.staticConfKeys.contains(entry.key),
      s"$entry is not registered")
    entry.readFrom(reader)
  }
}
