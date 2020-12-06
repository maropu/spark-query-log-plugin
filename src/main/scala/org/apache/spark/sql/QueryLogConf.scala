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
    .doc("Optimization level in LLVM.")
    .doubleConf
    .checkValue(v => 0.0 <= v && v <= 1.0, "The value must be in [0.0, 1.0].")
    .createWithDefault(1.0)
}

class QueryLogConf(conf: SQLConf) {
  import QueryLogConf._

  private val reader = new ConfigReader(conf.settings)

  def queryLogDbName: String = getConf(QUERY_LOG_DB_NAME)

  def queryLogTableName: String = getConf(QUERY_LOG_TABLE_NAME)

  def queryLogRandomSamplingRatio: Double = getConf(QUERY_LOG_RANDOM_SAMPLING_RATIO)

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
