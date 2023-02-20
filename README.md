# Big Data SQL Benchmark
This tool tests big data components by running some TPC-DS SQL. 

# Project Structure 

### hive-tpcds-setup
TPC-DS data generator.
If you want to test Hudi or Iceberg, you can use official tools to convert this data to Hudi or Iceberg.
Hudi tool: https://hudi.apache.org/docs/migration_guide
Iceberg tool: https://iceberg.apache.org/docs/latest/spark-procedures/#migrate

### jdbc-common
It tests TPC-DS via JDBC. You can run an [Apache Kyuubi][kyuubi-official-site] to expose JDBC service of Spark or Flink.

### spark-hudi
It tests TPC-DS SQL via a Spark SQL jar job.

[kyuubi-official-site]: https://kyuubi.apache.org/

### Acknowledgements

Inspired by https://github.com/ververica/flink-sql-benchmark