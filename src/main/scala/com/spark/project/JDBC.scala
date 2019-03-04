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
  val stTime = System.currentTimeMillis/1000
  val oracleJdbcDf = spark.read.format("jdbc")
    .option("url", "jdbc:oracle:thin:auditor/qwe123@172.17.0.3:1521:xe")
    .option("dbtable", "auditor.FETCH_SIZE_TEST")
    .option("fetchsize", "10")
    .option("driver", "oracle.jdbc.OracleDriver")
    .load()

  oracleJdbcDf.foreach(_ => ())
  val endTime = System.currentTimeMillis/1000
  println(s"Для fetchsize по умолчанию время работы ${(endTime-stTime)}")

  val base: Long = 10
  for {i <- 1 to 5

  }
    yield {
      val fetchsise = math.pow(base, i).toLong
      val stTime = System.currentTimeMillis/1000
      val oracleJdbcDfFS = spark.read.format("jdbc")
      .option("url", "jdbc:oracle:thin:auditor/qwe123@172.17.0.3:1521:xe")
      .option("dbtable", "auditor.FETCH_SIZE_TEST")
      .option("fetchsize", s"$fetchsise")
      .option("driver", "oracle.jdbc.OracleDriver")
      .load()
      oracleJdbcDfFS.foreach(_ => ())
      val endTime = System.currentTimeMillis/1000
      println(s"Для fetchsize = $fetchsise время работы ${(endTime-stTime)}")
    }

  //вернём как было
  Locale.setDefault(locale)
  /*val mssqlJdbcDf = spark.read.format("jdbc")
    .option("url", "jdbc:sqlserver://172.17.0.2:1433;database=audit;user=sa;password=*******;")
    .option("dbtable", "AUDIT_ITEMS")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()

  mssqlJdbcDf.printSchema()
  mssqlJdbcDf.show()*/
}
