package com.spark.project.auth

import java.sql.{Connection, DriverManager}

class MsSqlAuth(host: String, port: String, database: String, username: String, password: String) extends Auth {
  def getConnection(): Connection = {
    val url = "jdbc:sqlserver://" + host + ":" + port + ";database=" + database
    val connection: Connection = DriverManager.getConnection(url, username, password)
    connection
  }
}
