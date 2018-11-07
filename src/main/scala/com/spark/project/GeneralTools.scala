package com.spark.project

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait SparkContextClass {
  Logger.getLogger("org").setLevel(Level.ALL)

  val spark: SparkSession = SparkSession
    .builder()
    .appName("AppName")
    .config("spark.master", "local")
    .getOrCreate()

  spark//.sparkContext//.conf.set("spark.driver.memory", "6g")

}


class GeneralTools {

}
