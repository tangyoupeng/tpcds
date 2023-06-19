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

package com.databricks.spark.sql.perf.tpcds

import org.apache.spark.sql.SparkSession

case class GenTPCDSDataConfig(
                               dsdgenTools: String = null,
                               scaleFactor: String = null,
                               location: String = null,
                               format: String = null,
                               useDoubleForDecimal: Boolean = false,
                               useStringForDate: Boolean = false,
                               overwrite: Boolean = false,
                               partitionTables: Boolean = true,
                               clusterByPartitionColumns: Boolean = true,
                               filterOutNullPartitionValues: Boolean = true,
                               tableFilter: String = "",
                               numPartitions: Int = 100,
                               onlyWarn: Boolean = true
                             )

/**
 * Gen TPCDS data.
 * To run this:
 * {{{
 *   build/sbt "test:runMain <this class> -d <dsdgenTools> -s <scaleFactor> -l <location> -f <format>"
 * }}}
 */
object GenTPCDSData {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[GenTPCDSDataConfig]("Gen-TPC-DS-data") {
      opt[String]('d', "dsdgenTools")
        .action { (x, c) => c.copy(dsdgenTools = x) }
        .text("location of dsdgen")
        .required()
      opt[String]('s', "scaleFactor")
        .action((x, c) => c.copy(scaleFactor = x))
        .text("scaleFactor defines the size of the dataset to generate (in GB)")
        .required()
      opt[String]('l', "location")
        .action((x, c) => c.copy(location = x))
        .text("root directory of location to create data in")
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
      opt[Boolean]('o', "overwrite")
        .action((x, c) => c.copy(overwrite = x))
        .text("overwrite the data that is already there")
      opt[Boolean]('p', "partitionTables")
        .action((x, c) => c.copy(partitionTables = x))
        .text("create the partitioned fact tables")
      opt[Boolean]('c', "clusterByPartitionColumns")
        .action((x, c) => c.copy(clusterByPartitionColumns = x))
        .text("shuffle to get partitions coalesced into single files")
      opt[Boolean]('v', "filterOutNullPartitionValues")
        .action((x, c) => c.copy(filterOutNullPartitionValues = x))
        .text("true to filter out the partition with NULL key value")
      opt[String]('t', "tableFilter")
        .action((x, c) => c.copy(tableFilter = x))
        .text("\"\" means generate all tables")
      opt[Int]('n', "numPartitions")
        .action((x, c) => c.copy(numPartitions = x))
        .text("how many dsdgen partitions to run - number of input tasks.")
      opt[Boolean]('w', "onlyWarn")
        .action((x, c) => c.copy(onlyWarn = x))
        .text("print only warnings")
      help("help")
        .text("prints this usage text")
    }

    parser.parse(args, GenTPCDSDataConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  private def run(config: GenTPCDSDataConfig) {
    val spark = SparkSession
      .builder()
      .appName(getClass.getName)
      .getOrCreate()

    if (config.onlyWarn) {
      println(s"Only WARN")
      spark.sparkContext.setLogLevel("WARN")
    }

    spark.sparkContext.addFile(s"${config.dsdgenTools}")
    val tables = new TPCDSTables(spark.sqlContext,
      scaleFactor = config.scaleFactor,
      useDoubleForDecimal = config.useDoubleForDecimal,
      useStringForDate = config.useStringForDate)

    val start = System.currentTimeMillis()
    tables.genData(
      location = config.location,
      format = config.format,
      overwrite = config.overwrite,
      partitionTables = config.partitionTables,
      clusterByPartitionColumns = config.clusterByPartitionColumns,
      filterOutNullPartitionValues = config.filterOutNullPartitionValues,
      tableFilter = config.tableFilter,
      numPartitions = config.numPartitions)
    val end = System.currentTimeMillis()

    val resLine = s"Generate data, cost: ${(end - start) / 1000} seconds"
    println(resLine)
    val resultLocation = config.location.stripSuffix("/") + "/results"
    spark.sql(s"select '$resLine'").repartition(1).write.mode("overwrite").format("csv").save(s"$resultLocation/gendata")
    spark.stop()
  }
}
