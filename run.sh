#!/bin/sh
set -e

################# PARAMETERS #################
SCALE=100
FORMAT=parquet
ITERATIONS=2
LOCATION="jfs://demo/tmp/performance-datasets/tpcds/sf${SCALE}-parquet/"
DATABASE=tpcds_${FORMAT}_${SCALE}_jfs
FILTER_QUERIES="q1-v2.4,q2-v2.4,q3-v2.4"
ENABLE_HIVE=false

ENABLE_KERBEROS=false
KEYTAB=/root/hdfs.keytab
PRINCIPAL=hdfs

SPARK_CONF="
--master yarn
--deploy-mode client
--driver-memory 4g
--executor-memory 8G
--executor-cores 4
--num-executors 4
--conf spark.sql.adaptive.enabled=true
--conf spark.driver.memoryOverhead=1g
--conf spark.executor.memoryOverhead=2g
"

################# PARAMETERS #################

CURRENT_DIR=$(cd `dirname $0`; pwd)
cd ${CURRENT_DIR}

if $ENABLE_KERBEROS; then
    SPARK_CONF="${SPARK_CONF} --keytab ${KEYTAB} --principal ${PRINCIPAL}"
fi

set -x
# Generate data for tpcds
/opt/spark/bin/spark-submit ${SPARK_CONF} \
  --class com.databricks.spark.sql.perf.tpcds.GenTPCDSData \
  spark-sql-perf/target/scala-2.12/spark-sql-perf-assembly-0.5.2-SNAPSHOT.jar \
  --dsdgenTools tpcds-kit/tools.tar.gz \
  --scaleFactor ${SCALE} \
  --location ${LOCATION} \
  --format ${FORMAT}

# Run tpcds benchmark
/opt/spark/bin/spark-submit ${SPARK_CONF} \
  --class com.databricks.spark.sql.perf.tpcds.RunTPCDS \
  spark-sql-perf/target/scala-2.12/spark-sql-perf-assembly-0.5.2-SNAPSHOT.jar \
  --location ${LOCATION} \
  --database ${DATABASE} \
  --format ${FORMAT} \
  --iterations ${ITERATIONS} \
  --filterQueries ${FILTER_QUERIES} \
  --enableHive ${ENABLE_HIVE}
