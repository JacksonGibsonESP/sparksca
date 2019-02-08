package com.spark.project

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait SparkContextClass {

  val total_general_path = "/home/boris/Рабочий стол/Themes/!Spark/SparkScalaCource"

  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.INFO)

// val res1 = "local[*]"


    val spark: SparkSession = SparkSession
      .builder.master("local[*]")
      .appName("AppName")
      .config("spark.network.timeout", "10000000")
      .config("spark.logConf", "true")
      .config("spark.dynamicAllolcation.enabled", "$res1")
      .config("spark.executor.extraJavaOptions", "-Xss30m")
      .config("spark.driver.extraJavaOptions", "-Xss30m")
      .config("blablabab", "blabalakf")
      .getOrCreate() // Параметр создает новую сессию или использует уже существующую

    spark


  //.sparkContext//.conf.set("spark.driver.memory", "6g")

}


class GeneralTools {

}
