package com.spark.project

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait SparkContextClass {

  val total_general_path = "/home/boris/Рабочий стол/Themes/!Spark/SparkScalaCource"

  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val spark:SparkSession = SparkSession
    .builder.master("local[*]")
    .appName("AppName")
    .getOrCreate() // Параметр создает новую сессию или использует уже существующую

  spark//.sparkContext//.conf.set("spark.driver.memory", "6g")

}


class GeneralTools {

}
