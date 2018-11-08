package com.spark.project

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait SparkContextClass {

  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val spark:SparkSession = SparkSession
    .builder.master("local[*]")
    .appName("AppName").getOrCreate()

  spark//.sparkContext//.conf.set("spark.driver.memory", "6g")

}


class GeneralTools {

}
