package com.databricks.spark.sql.perf.tpcds

case class RunTPCDSConfig(
                           location: String = null,
                           database: String = null,
                           format: String = null,
                           useDoubleForDecimal: Boolean = false,
                           useStringForDate: Boolean = false,
                           iterations: Int = 2,
                           optimizeQueries: Boolean = false,
                           filterQueries: String = "",
                           onlyWarn: Boolean = true,
                           enableHive: Boolean = false
                         )

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object RunTPCDS {
  def main(args: Array[String]) {
    val parser = new scopt.OptionParser[RunTPCDSConfig]("Run-TPC-DS") {
      opt[String]('l', "location")
        .action((x, c) => c.copy(location = x))
        .text("root directory of location to create data in")
        .required()
      opt[String]('d', "database")
        .action((x, c) => c.copy(database = x))
        .text("scaleFactor defines the size of the dataset to generate (in GB)")
        .required()
      opt[String]('f', "format")
        .action((x, c) => c.copy(format = x))
        .text("valid spark format, Parquet, ORC ...")
        .required()
      opt[Boolean]('i', "useDoubleForDecimal")
        .action((x, c) => c.copy(useDoubleForDecimal = x))
        .text("true to replace DecimalType with DoubleType")
      opt[Boolean]('e', "useStringForDate")
        .action((x, c) => c.copy(useStringForDate = x))
        .text("true to replace DateType with StringType")
      opt[Int]('n', "iterations")
        .action((x, c) => c.copy(iterations = x))
        .text("how many times to run the whole set of queries")
      opt[Boolean]('o', "optimizeQueries")
        .action((x, c) => c.copy(optimizeQueries = x))
        .text("optimize queries")
      opt[String]('q', "filterQueries")
        .action((x, c) => c.copy(filterQueries = x))
        .text("query filter, \"q1-v2.4,q2-v2.4,...\"")
      opt[Boolean]('w', "onlyWarn")
        .action((x, c) => c.copy(onlyWarn = x))
        .text("print only warnings")
      opt[Boolean]('b', "enableHive")
        .action((x, c) => c.copy(enableHive = x))
        .text("enable hive support")
      help("help")
        .text("prints this usage text")
    }

    parser.parse(args, RunTPCDSConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  private def run(config: RunTPCDSConfig) {
    val location = config.location.stripSuffix("/")
    val database = config.database
    val resultLocation = location + "/" + "results"
    val format = config.format
    val iterations = config.iterations
    val optimizeQueries = config.optimizeQueries
    val filterQueries = config.filterQueries

    val scheme = new Path(location).toUri.getScheme
    println(s"Database name is $database")

    val timeout = 24 * 60 * 60

    println(s"DATA DIR is $location")

    val builder = SparkSession
      .builder
      .appName(s"TPCDS SQL Benchmark $database")
    if (config.enableHive) {
      builder.enableHiveSupport
    }
    val spark = builder.getOrCreate()

    if (config.onlyWarn) {
      println(s"Only WARN")
      spark.sparkContext.setLogLevel("WARN")
    }

    val tables = new TPCDSTables(spark.sqlContext,
      scaleFactor = "",
      useDoubleForDecimal = config.useDoubleForDecimal,
      useStringForDate = config.useStringForDate)

    val start = System.currentTimeMillis()
    tables.createExternalTables(location, format, database, overwrite = true, discoverPartitions = true)
    val end = System.currentTimeMillis()
    println(s"Create tables, cost: ${(end - start) / 1000} seconds")

    if (optimizeQueries) {
      println("Optimizing queries")
      val start1 = System.currentTimeMillis()
      tables.analyzeTables(database, analyzeColumns = true)
      val end1 = System.currentTimeMillis()
      println(s"Optimize tables, cost: ${(end1 - start1) / 1000} seconds")
    }

    val tpcds = new TPCDS(spark.sqlContext)

    var query_filter: Seq[String] = Seq()
    if (!filterQueries.isEmpty) {
      println(s"Running only queries: $filterQueries")
      query_filter = filterQueries.split(",").toSeq
    }

    val filtered_queries = query_filter match {
      case Seq() => tpcds.tpcds2_4Queries
      case _ => tpcds.tpcds2_4Queries.filter(q => query_filter.contains(q.name))
    }

    // Start experiment
    val experiment = tpcds.runExperiment(
      filtered_queries,
      iterations = iterations,
      resultLocation = resultLocation,
      forkThread = true)

    experiment.waitForFinish(timeout)

    // Collect general results
    val resultPath = experiment.resultPath
    println(s"Reading result at $resultPath")
    val specificResultTable = spark.read.json(resultPath)
    specificResultTable.show()

    // Summarize results
    val result = specificResultTable
      .withColumn("result", explode(col("results")))
      .withColumn("runtimeSeconds", (
        col("result.parsingTime")
          + col("result.analysisTime")
          + col("result.planningTime")
          + col("result.executionTime")) / 1000)
      .withColumn("queryName", col("result.name"))
    result.select("iteration", "queryName", "runtimeSeconds").show()
    println(s"Final results at $resultPath")

    val aggResults = result.groupBy("queryName").agg(
      callUDF("percentile", col("runtimeSeconds").cast("long"), lit(0.5)).as('medianRuntimeSeconds),
      callUDF("min", col("runtimeSeconds").cast("long")).as('minRuntimeSeconds),
      callUDF("max", col("runtimeSeconds").cast("long")).as('maxRuntimeSeconds)
    ).orderBy(col("queryName"))
    aggResults.repartition(1).write.csv(s"$resultPath/summary.csv")
    aggResults.show(105)

    spark.stop()
  }
}