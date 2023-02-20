package spark3.sql

import org.apache.hudi.com.beust.jcommander.JCommander
import org.apache.spark.sql.SparkSession
import scala.io.Source

object SqlWrapper {
  def main(args: Array[String]): Unit = {

    val opt = new CliOptions
    val cmd = new JCommander(opt, null, args: _*)

    val spark = SparkSession
      .builder()
      .appName("Spark Hudi Wrapper")
//      .config("spark.master", "local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .enableHiveSupport()
      .getOrCreate()
//    import spark.implicits._
    spark.sparkContext.hadoopConfiguration.addResource("core-site.xml")
    spark.sparkContext.hadoopConfiguration.addResource("hdfs-site.xml")

    val start = System.nanoTime()

    val sqlFileName = s"q${opt.queries}.sql"

    val path: String = "queries/" + sqlFileName
    val tpsSql = Source.fromResource(path).mkString
    spark.sql(s"use ${opt.database}")
    println(s"Database: ${opt.database}, SQL file: $sqlFileName")
    val df = spark.sql(tpsSql.stripMargin)
    df.show()
    val elapsed = (System.nanoTime() - start) / 1000000000.0
    println(s"Elapsed time: $elapsed seconds")
    spark.stop()
  }
}
