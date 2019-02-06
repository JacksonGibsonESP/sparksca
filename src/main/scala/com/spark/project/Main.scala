package com.spark.project

import com.spark.project.auth.AuthSource
import com.spark.project.parameters.Parameters._
import com.spark.project.parameters.ParametersHandler

object Main extends App {
  val argHandler = new ParametersHandler("local")
  val parameters = argHandler.getParameters
  print("Считанные параметры: ")
  println(parameters.mkString(", "))

  val jdbcAuth = new AuthSource(
    parameters(JDBC_HOSTNAME),
    parameters(JDBC_PORT),
    parameters(JDBC_SID),
    parameters(JDBC_USERNAME),
    parameters(JDBC_USERPASSWORD))

  val connection = jdbcAuth.getConnection()

  try {

    var sourceHandler = new SourceHandler(connection, parameters(TABLENAME))

    sourceHandler.checkSource()

    var resultSet = sourceHandler.getResultSet()

    while (resultSet.next()) {
      val id = resultSet.getLong("id")
      val name = resultSet.getString("name")
      val dt = resultSet.getTimestamp("dt")
      println("id, name, dt: " + id + ", " + name + ", " + dt)
    }

    if (parameters.get(INCRFIELD).nonEmpty) {
      val sourceHandler = new SourceHandler(connection, parameters(TABLENAME), parameters(INCRFIELD))

      sourceHandler.checkSource()

      val resultSet = sourceHandler.getResultSet()

      while (resultSet.next()) {
        val id = resultSet.getLong("id")
        val name = resultSet.getString("name")
        val dt = resultSet.getTimestamp("dt")
        println("id, name, dt: " + id + ", " + name + ", " + dt)
      }
    }
  } catch {
    case e: Throwable =>
      println("Ошибка во время установки соединения с базой данных.")
      throw e
  } finally {
    connection.close()
  }
}
