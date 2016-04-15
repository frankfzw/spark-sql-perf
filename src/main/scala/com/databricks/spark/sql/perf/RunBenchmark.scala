/*
 * Copyright 2015 Databricks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.sql.perf

import java.net.InetAddress

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkContext, SparkConf}

import com.databricks.spark.sql.perf.tpcds.{Tables, TPCDS}

import scala.util.Try

case class RunConfig(
    benchmarkName: String = null,
    filter: Option[String] = None,
    iterations: Int = 3,
    baseline: Option[Long] = None,
    dsdgenDir: String = null,
    scaleFactor: Int = 1,
    format: String = "json")

/**
 * Runs a benchmark locally and prints the results to the screen.
 */
object RunBenchmark {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[RunConfig]("spark-sql-perf") {
      head("spark-sql-perf", "0.2.0")
      opt[String]('b', "benchmark")
        .action { (x, c) => c.copy(benchmarkName = x) }
        .text("the name of the benchmark to run")
        .required()
      opt[String]('f', "filter")
        .action((x, c) => c.copy(filter = Some(x)))
        .text("a filter on the name of the queries to run")
      opt[Int]('i', "iterations")
        .action((x, c) => c.copy(iterations = x))
        .text("the number of iterations to run")
      opt[Long]('c', "compare")
          .action((x, c) => c.copy(baseline = Some(x)))
          .text("the timestamp of the baseline experiment to compare with")
      opt[String]('p', "dsdgenDir")
          .action((x, c) => c.copy(dsdgenDir = x))
          .text("the local path of dsdgen")
      opt[String]('r', "format")
          .action((x, c) => c.copy(format = x))
          .text("the local path of dsdgen")
      opt[Int]('s', "scaleFactor")
          .action((x, c) => c.copy(scaleFactor = x))
          .text("the scale factor of dsdgen")
      help("help")
        .text("prints this usage text")
    }

    parser.parse(args, RunConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  def run(config: RunConfig): Unit = {
    val conf = new SparkConf()
        .setAppName(getClass.getName)

    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    sqlContext.setConf("spark.sql.perf.results", new java.io.File("performance").toURI.toString)
    println("== frankfzw Generate Data ==")
    val dsdgenDir = config.dsdgenDir
    val scaleFactor = config.scaleFactor
    val tables = new Tables(sqlContext, dsdgenDir, scaleFactor)
    val location = "hdfs:///mnt/tmp"
    val format = config.format
    val overwrite = true
    val partitionTables = false
    val useDoubleForDecimal = false
    val clusterByPartitionColumns = false
    val filterOutNullPartitionValues = true
    tables.genData(location, format, overwrite, partitionTables, useDoubleForDecimal, clusterByPartitionColumns, filterOutNullPartitionValues)
    tables.createTemporaryTables(location, format)
    val benchmark = new TPCDS (sqlContext = sqlContext)
    // val benchmark = Try {
    //   Class.forName(config.benchmarkName)
    //       .newInstance()
    //       .asInstanceOf[Benchmark]
    // } getOrElse {
    //   Class.forName("com.databricks.spark.sql.perf." + config.benchmarkName)
    //       .newInstance()
    //       .asInstanceOf[Benchmark]
    // }

    // val allQueries = config.filter.map { f =>
    //   benchmark.allQueries.filter(_.name contains f)
    // } getOrElse {
    //   benchmark.allQueries
    // }
    val allQueries = benchmark.tpcds1_4Queries

    println("== QUERY LIST ==")
    allQueries.foreach(println)

    // val experiment = benchmark.runExperiment(
    //   executionsToRun = allQueries,
    //   iterations = config.iterations,
    //   tags = Map(
    //     "runtype" -> "local",
    //     "host" -> InetAddress.getLocalHost().getHostName()))
    val experiment = benchmark.runExperiment(allQueries)

    println("== STARTING EXPERIMENT ==")
    experiment.waitForFinish(1000 * 60 * 30)

    sqlContext.setConf("spark.sql.shuffle.partitions", "1")
    experiment.getCurrentRuns()
        .withColumn("result", explode($"results"))
        .select("result.*")
        .groupBy("name")
        .agg(
          min($"executionTime") as 'minTimeMs,
          max($"executionTime") as 'maxTimeMs,
          avg($"executionTime") as 'avgTimeMs,
          stddev($"executionTime") as 'stdDev)
        .orderBy("name")
        .show(truncate = false)
    println(s"""Results: sqlContext.read.json("${experiment.resultPath}")""")

    config.baseline.foreach { baseTimestamp =>
      val baselineTime = when($"timestamp" === baseTimestamp, $"executionTime").otherwise(null)
      val thisRunTime = when($"timestamp" === experiment.timestamp, $"executionTime").otherwise(null)

      val data = sqlContext.read.json(benchmark.resultsLocation)
          .coalesce(1)
          .where(s"timestamp IN ($baseTimestamp, ${experiment.timestamp})")
          .withColumn("result", explode($"results"))
          .select("timestamp", "result.*")
          .groupBy("name")
          .agg(
            avg(baselineTime) as 'baselineTimeMs,
            avg(thisRunTime) as 'thisRunTimeMs,
            stddev(baselineTime) as 'stddev)
          .withColumn(
            "percentChange", ($"baselineTimeMs" - $"thisRunTimeMs") / $"baselineTimeMs" * 100)
          .filter('thisRunTimeMs.isNotNull)

      data.show(truncate = false)
    }
  }
}
