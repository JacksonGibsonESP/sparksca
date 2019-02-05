package com.spark.project.auth

import java.util.Locale
import java.sql.DriverManager
import java.sql.Connection

class AuthIdentifier {
  val driver = "oracle.jdbc.OracleDriver"
  //Thin driver, host localhost, port 49161, SID xe
  val url = "jdbc:oracle:thin:@localhost:49161:xe"
  val username = "auditor"
  val password = "qwe123"

  //register jdbc driver
  Class.forName(driver)

  val locale = Locale.getDefault
  //иначе ошибка ORA-12705
  Locale.setDefault(Locale.ENGLISH)

  val connection: Connection = DriverManager.getConnection(url, username, password)
  try {
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery("SELECT * FROM audit_items")
    while (resultSet.next()) {
      val id = resultSet.getLong("id")
      val name = resultSet.getString("name")
      val dt = resultSet.getTimestamp("dt")
      println("id, name, dt: " + id + ", " + name + ", " + dt)
    }
  } finally {
    connection.close()
  }

  //вернём как было
  Locale.setDefault(locale)
}
