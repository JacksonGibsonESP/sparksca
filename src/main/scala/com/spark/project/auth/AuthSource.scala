package com.spark.project.auth

import java.util.Locale
import java.sql.DriverManager
import java.sql.Connection

class AuthSource(val host: String, val port: String, val sid: String, val username: String, val password: String) extends Auth {

  def getConnection(): Connection = {
    val driver = "oracle.jdbc.OracleDriver"
    //  Thin driver
    val url = "jdbc:oracle:thin:@" + host + ":" + port + ":" + sid

    //register jdbc driver
    Class.forName(driver)

    val locale = Locale.getDefault
    //иначе ошибка ORA-12705
    Locale.setDefault(Locale.ENGLISH)

    val connection: Connection = DriverManager.getConnection(url, username, password)

    //вернём как было
    Locale.setDefault(locale)

    connection
  }
}
