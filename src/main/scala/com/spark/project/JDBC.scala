package com.spark.project

import java.util.Locale

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object JDBC extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

//  val driver = "oracle.jdbc.OracleDriver"
//  register jdbc driver
//  Class.forName(driver)

  val spark = SparkSession
    .builder()
//    .master("local[1]")
    .appName("Spark_JDBC_connection")
    .getOrCreate()

  val locale = Locale.getDefault
  //иначе ошибка ORA-12705
  Locale.setDefault(Locale.ENGLISH)

  val oracleJdbcDf = spark.read.format("jdbc")
    .option("url", "jdbc:oracle:thin:auditor/qwe123@172.17.0.3:1521:xe")
    .option("dbtable", "auditor.AUDIT_ITEMS")
    .option("driver", "oracle.jdbc.OracleDriver")
    .load()

  oracleJdbcDf.printSchema()
  oracleJdbcDf.show()

  //вернём как было
  Locale.setDefault(locale)

  val mssqlJdbcDf = spark.read.format("jdbc")
    .option("url", "jdbc:sqlserver://172.17.0.2:1433;database=audit;user=sa;password=*******;")
    .option("dbtable", "AUDIT_ITEMS")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()

  mssqlJdbcDf.printSchema()
  mssqlJdbcDf.show()
}
