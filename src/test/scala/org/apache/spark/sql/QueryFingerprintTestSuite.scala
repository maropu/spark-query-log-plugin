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

import java.io.File
import java.net.URI

import io.github.maropu.spark.regularizer.Regularizer

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.QueryLogUtils
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.{fileToString, stringToFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class QueryFingerprintTestSuite extends QueryTest with SharedSparkSession with SQLHelper {

  private val regenerateGoldenFiles: Boolean = System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"

  private def getWorkspaceFilePath(first: String, more: String*) = {
    if (!(sys.props.contains("spark.test.home") || sys.env.contains("SPARK_HOME"))) {
      fail("spark.test.home or SPARK_HOME is not set.")
    }
    val sparkHome = sys.props.getOrElse("spark.test.home", sys.env("SPARK_HOME"))
    java.nio.file.Paths.get(sparkHome, first +: more: _*)
  }

  protected val baseResourcePath = {
    getWorkspaceFilePath("src", "test", "resources", "fingerprint-tests").toFile
  }

  protected val inputFilePath = new File(baseResourcePath, "inputs").getAbsolutePath
  protected val goldenFilePath = new File(baseResourcePath, "results").getAbsolutePath

  protected val validFileExtensions = ".sql"

  protected override def sparkConf: SparkConf = super.sparkConf
    // Fewer shuffle partitions to speed up testing.
    .set(SQLConf.SHUFFLE_PARTITIONS, 4)

  // Create all the test cases.
  listTestCases.foreach(createScalaTestCase)

  /** A single SQL query's output. */
  protected case class QueryOutput(sql: String, plan: String, fingerprint: Int) {
    override def toString: String = {
      // We are explicitly not using multi-line string due to stripMargin removing "|" in output.
      s"-- !query\n" +
        sql + "\n" +
        s"-- !query plan\n" +
        plan + "\n" +
        s"-- !query fingerprint\n" +
        fingerprint
    }
  }

  /** A regular test case. */
  protected case class TestCase(name: String, inputFile: String, resultFile: String)

  protected def createScalaTestCase(testCase: TestCase): Unit = {
    // Create a test case to run this case.
    test(testCase.name) {
      runTest(testCase)
    }
  }

  /** Run a test case. */
  protected def runTest(testCase: TestCase): Unit = {
    def splitWithSemicolon(seq: Seq[String]) = {
      seq.mkString("\n").split("(?<=[^\\\\]);")
    }

    def splitCommentsAndCodes(input: String) = {
      input.split("\n").partition(_.trim.startsWith("--"))
    }

    val input = fileToString(new File(testCase.inputFile))
    val (_, code) = splitCommentsAndCodes(input)

    // List of SQL queries to run
    val queries = splitWithSemicolon(code).toSeq.map(_.trim).filter(_ != "").toSeq
      // Fix misplacement when comment is at the end of the query.
      .map(_.split("\n").filterNot(_.startsWith("--")).mkString("\n")).map(_.trim).filter(_ != "")

    runQueries(queries, testCase)
  }

  private val notIncludedMsg = "[not included in comparison]"
  private val clsName = this.getClass.getCanonicalName

  private def replaceNotIncludedMsg(line: String): String = {
    line.replaceAll("#\\d+", "#x")
      .replaceAll(s"Location.*$clsName/", s"Location $notIncludedMsg/{warehouse_dir}/")
      .replaceAll("Created By.*", s"Created By $notIncludedMsg")
      .replaceAll("Created Time.*", s"Created Time $notIncludedMsg")
      .replaceAll("Last Access.*", s"Last Access $notIncludedMsg")
      .replaceAll("Partition Statistics\t\\d+", s"Partition Statistics\t$notIncludedMsg")
  }

  protected def runQueries(queries: Seq[String], testCase: TestCase): Unit = {
    // Create a local SparkSession to have stronger isolation between different test cases.
    // This does not isolate catalog changes.
    val localSparkSession = spark.newSession()

    // Run the SQL queries preparing them for comparison.
    val outputs: Seq[QueryOutput] = queries.map { sql =>
      val p = Regularizer.execute(localSparkSession.sql(sql).queryExecution.optimizedPlan)
      val fingerprint = QueryLogUtils.computeFingerprint(p)
      QueryOutput(sql, replaceNotIncludedMsg(p.toString.trim), fingerprint)
    }

    if (regenerateGoldenFiles) {
      // Again, we are explicitly not using multi-line string due to stripMargin removing "|".
      val goldenOutput = {
        s"-- Automatically generated by ${getClass.getSimpleName}\n" +
          s"-- Number of queries: ${outputs.size}\n\n\n" +
          outputs.zipWithIndex.map{case (qr, _) => qr.toString}.mkString("\n\n\n") + "\n"
      }
      val resultFile = new File(testCase.resultFile)
      val parent = resultFile.getParentFile
      if (!parent.exists()) {
        assert(parent.mkdirs(), "Could not create directory: " + parent)
      }
      stringToFile(resultFile, goldenOutput)
    }

    withClue(s"${testCase.name}${System.lineSeparator()}") {
      // Read back the golden file.
      val expectedOutputs: Seq[QueryOutput] = {
        val goldenOutput = fileToString(new File(testCase.resultFile))
        val segments = goldenOutput.split("-- !query.*\n")

        // each query has 3 segments, plus the header
        assert(segments.size == outputs.size * 3 + 1,
          s"Expected ${outputs.size * 3 + 1} blocks in result file but got ${segments.size}. " +
            s"Try regenerate the result files.")
        Seq.tabulate(outputs.size) { i =>
          QueryOutput(
            sql = segments(i * 3 + 1).trim,
            plan = segments(i * 3 + 2).trim,
            fingerprint = segments(i * 3 + 3).trim.toInt
          )
        }
      }

      // Compare results.
      assertResult(expectedOutputs.size, s"Number of queries should be ${expectedOutputs.size}") {
        outputs.size
      }

      outputs.zip(expectedOutputs).zipWithIndex.foreach { case ((output, expected), i) =>
        assertResult(expected.sql, s"SQL query did not match for query #$i\n${expected.sql}") {
          output.sql
        }
        assertResult(expected.plan, s"Plan did not match for query #$i\n${expected.sql}") {
          output.plan
        }
        assertResult(expected.fingerprint, s"Fingerprint did not match" +
          s" for query #$i\n${expected.sql}") { output.fingerprint }
      }
    }
  }

  protected lazy val listTestCases: Seq[TestCase] = {
    listFilesRecursively(new File(inputFilePath)).flatMap { file =>
      val resultFile = file.getAbsolutePath.replace(inputFilePath, goldenFilePath) + ".out"
      val absPath = file.getAbsolutePath
      val testCaseName = absPath.stripPrefix(inputFilePath).stripPrefix(File.separator)
      TestCase(testCaseName, absPath, resultFile) :: Nil
    }
  }

  /** Returns all the files (not directories) in a directory, recursively. */
  protected def listFilesRecursively(path: File): Seq[File] = {
    val (dirs, files) = path.listFiles().partition(_.isDirectory)
    // Filter out test files with invalid extensions such as temp files created
    // by vi (.swp), Mac (.DS_Store) etc.
    val filteredFiles = files.filter(_.getName.endsWith(validFileExtensions))
    filteredFiles ++ dirs.flatMap(listFilesRecursively)
  }

  /** Load built-in test tables into the SparkSession. */
  private def createTestTables(session: SparkSession): Unit = {
    import session.implicits._

    // Before creating test tables, deletes orphan directories in warehouse dir
    Seq("testdata").foreach { dirName =>
      val f = new File(new URI(s"${conf.warehousePath}/$dirName"))
      if (f.exists()) {
        Utils.deleteRecursively(f)
      }
    }

    (1 to 100).map(i => (i, i.toString)).toDF("key", "value")
      .repartition(1)
      .write
      .format("parquet")
      .saveAsTable("testdata")
  }

  private def removeTestTables(session: SparkSession): Unit = {
    session.sql("DROP TABLE IF EXISTS testdata")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTestTables(spark)
  }

  override def afterAll(): Unit = {
    try {
      removeTestTables(spark)
    } finally {
      super.afterAll()
    }
  }
}
