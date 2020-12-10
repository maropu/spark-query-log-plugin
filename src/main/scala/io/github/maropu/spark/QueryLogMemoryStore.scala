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

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, SparkSession}

// For test uses
private[spark] class QueryLogMemoryStore extends QueryLogStore {

  private var queryLogs = mutable.ArrayBuffer[QueryLog]()

  override def init(): Unit = {}

  override def put(queryLog: QueryLog): Unit = synchronized {
    queryLogs.append(queryLog)
  }

  override def load(): DataFrame = synchronized {
    SparkSession.getActiveSession.map { sparkSession =>
      sparkSession.createDataFrame(queryLogs.clone)
    }.getOrElse {
      throw new SparkException("Active Spark session not found")
    }
  }

  override def reset(): Unit = synchronized {
    queryLogs = mutable.ArrayBuffer[QueryLog]()
  }
}
