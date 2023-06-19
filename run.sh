#!/bin/sh
set -e

CURRENT_DIR=$(cd `dirname $0`; pwd)
cd ${CURRENT_DIR}

SCALE=2
FORMAT=parquet
LOCATION="jfs://demo/tmp/performance-datasets/tpcds/sf${SCALE}-parquet/"
DATABASE=tpcds_${FORMAT}_${SCALE}_jfs

SPARK_CONF="
--master yarn
--deploy-mode client
--driver-memory 4g
--executor-memory 8G
--executor-cores 4
--conf spark.sql.shuffle.partitions=10
--conf spark.driver.memoryOverhead=1g
--conf spark.executor.memoryOverhead=2g
"

set -x
# Generate data for tpcds
/opt/spark/bin/spark-submit ${SPARK_CONF} \
  --class com.databricks.spark.sql.perf.tpcds.GenTPCDSData \
  spark-sql-perf/target/scala-2.12/spark-sql-perf-assembly-0.5.2-SNAPSHOT.jar \
  --dsdgenTools tpcds-kit/tools.tar.gz \
  --scaleFactor 2 \
  --location "jfs://demo/tmp/performance-datasets/tpcds/sf2-parquet/" \
  --format parquet

# Run tpcds benchmark
/opt/spark/bin/spark-submit ${SPARK_CONF} \
  --class com.databricks.spark.sql.perf.tpcds.RunTPCDS \
  spark-sql-perf/target/scala-2.12/spark-sql-perf-assembly-0.5.2-SNAPSHOT.jar \
  --location "jfs://demo/tmp/performance-datasets/tpcds/sf2-parquet/" \
  --database tpcds_parquet_2_jfs \
  --format parquet \
  --iterations 2 \
  --filterQueries  "q1-v2.4,q2-v2.4,q3-v2.4"
#  --enableHive true
