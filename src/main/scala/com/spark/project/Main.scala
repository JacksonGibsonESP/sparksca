package com.spark.project

import com.spark.project.auth.AuthSource
import com.spark.project.parameters.Parameters._
import com.spark.project.parameters.ParametersHandler

object Main extends App {
  val argHandler = new ParametersHandler("local")
  val parameters = argHandler.getParameters
  print("Считанные параметры: ")
  println(parameters.mkString(", "))

//  println(parameters.get(AGE))

  val jdbcAuth = new AuthSource(
    parameters(JDBC_HOSTNAME),
    parameters(JDBC_PORT),
    parameters(JDBC_SID),
    parameters(JDBC_USERNAME),
    parameters(JDBC_USERPASSWORD))

  try {
    val connection = jdbcAuth.getConnection()

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
  } catch {
    case e: Throwable =>
      println("Ошибка во время установки соединения с базой данных.")
      throw e
  }
}
