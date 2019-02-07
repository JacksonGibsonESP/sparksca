package com.spark.project
import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, round, substring, to_date, unix_timestamp}

import scala.io.Codec

object Test extends App {
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  if (args.length == 1) {
//      println(s"${args(0)}")

//    implicit val codec = Codec("CP1251")
//    codec.onMalformedInput(CodingErrorAction.REPLACE)
//    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

      val spark = SparkSession
//        .builder.master("local[1]")
        .builder()
        .appName("Test")
        .getOrCreate()

      val path: String = args(0)

    println(path)

      val df = spark.read
        .format("com.databricks.spark.csv")
        .option("delimiter", ";")
        .option("encoding", "CP1251")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path)


      val res = df
        .withColumn("msg", substring(col("msg"), 2, 4))
        .withColumn("summ", round(col("summ").divide(2)))
        .withColumn("date", date_format(col("date"), "01-MM-yyyy"))
//        .withColumn("date", to_date(unix_timestamp(col("date"), "yyyy-MM").cast("timestamp")))
      res.show()
      res.printSchema()

      val options = Map("path" -> "/tmp/test") // for me every database has a different warehouse. I am not using the default warehouse. I am using users' directory for warehousing DBs and tables
      //and simply write it!
      res.write.options(options).saveAsTable("db_name.table_name")

//      res.createGlobalTempView("test")

//      res.explain(true)

//      spark.sql("SELECT * FROM global_temp.test").show()
//      spark.sql("EXPLAIN SELECT * FROM global_temp.test").show(500, false)
  }
  else
    println("No arguments provided")
}
