package com.spark.project.auth

import java.sql.{Connection, DriverManager}
import java.util.Locale

class OracleAuth(host: String, port: String, sid: String, username: String, password: String) extends Auth {
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
