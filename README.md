# Spark SQL TPCDS Tests
## build spark-sql-perf
```shell
cd spark-sql-perf
sbt 'set test in assembly := {}' clean assembly
```
## run tpcds

Modify the parameters in script `run.sh` before run it.

If you want to also save table metadata to hive. Use `--enableHive true` for tpcds benchmark.

```shell
./run.sh
```

After the benchmark success. 
The result will be printed the console and save to $location/results.