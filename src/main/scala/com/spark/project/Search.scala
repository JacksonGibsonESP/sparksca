package com.spark.project

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Search extends App {
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  if (args.length > 0) {
    val spark = SparkSession
      .builder()
      .appName("Search")
      .getOrCreate()

    val scheme: String = args(0)
    println(scheme)

    spark.sql("USE " + scheme)

    val tables = spark.sql("SHOW TABLES")
    import spark.implicits._

    val tableNames = tables.select("tableName").as[String].collect()


    for (tableName <- tableNames) {
      println(tableName + ':')
      val tableDescription = spark.sql("DESCRIBE " + tableName)
      val columnNames = tableDescription.filter(($"col_name".contains("date") || $"col_name".contains("dt")) && ($"data_type" === "date" || $"data_type" === "timestamp")).select("col_name").as[String].collect()
      println(columnNames.mkString(", "))
    }
  }
  else
    println("No arguments provided")
}
