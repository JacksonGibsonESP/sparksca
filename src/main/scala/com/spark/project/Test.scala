package com.spark.project

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, round, substring}


object Test extends App {
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  if (args.length > 0) {
    val spark = SparkSession
//        .builder.master("local[1]")
      .builder()
      .appName("Test")
      .getOrCreate()

    val path: String = args(0)
    println(path)

    //CP866, CP1251, UTF8
    val encoding: String = args(1)
    println(encoding)

    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("encoding", encoding)
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)

    val res = df
      .withColumn("msg", substring(col("msg"), 2, 4))
      .withColumn("summ", round(col("summ").divide(2)))
      .withColumn("date", date_format(col("date"), "01-MM-yyyy"))
//      .withColumn("date", to_date(unix_timestamp(col("date"), "yyyy-MM").cast("timestamp")))
    res.show()
    res.printSchema()

    spark.sql("CREATE DATABASE IF NOT EXISTS test")
    spark.sql("DROP TABLE IF EXISTS test.table_test")

    val options = Map("path" -> "/tmp/test")
    res.write.options(options).saveAsTable("test.table_test")

//    res.createGlobalTempView("test")
//
//    res.explain(true)
//
//    spark.sql("SELECT * FROM global_temp.test").show()
//    spark.sql("EXPLAIN SELECT * FROM global_temp.test").show(500, false)
  }
  else
    println("No arguments provided")
}
