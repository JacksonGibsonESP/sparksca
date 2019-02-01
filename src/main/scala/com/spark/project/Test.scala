package com.spark.project
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, substring, round, date_format, to_date, unix_timestamp}

object Test extends App {
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  if (args.length == 1) {
//      println(s"${args(0)}")

      val spark = SparkSession
        .builder.master("local[1]")
        .appName("Test")
        .getOrCreate()

      val path: String = args(0)

    println(path)

      val df = spark.read
        .option("delimiter", ";")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)


      val res = df
        .withColumn("msg", substring(col("msg"), 2, 4))
        .withColumn("summ", round(col("summ").divide(2)))
        .withColumn("date", date_format(col("date"), "01-MM-yyyy"))
//        .withColumn("date", to_date(unix_timestamp(col("date"), "yyyy-MM").cast("timestamp")))
      res.show()
      res.printSchema()

      res.createGlobalTempView("test")

      res.explain(true)

      spark.sql("SELECT * FROM global_temp.test").show()
      spark.sql("EXPLAIN SELECT * FROM global_temp.test").show(500, false)
  }
  else
    println("No arguments provided")
}
