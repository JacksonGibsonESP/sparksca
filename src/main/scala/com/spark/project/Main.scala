package com.spark.project

import com.spark.project.auth.AuthSource
import com.spark.project.parameters.Parameters._
import com.spark.project.parameters.ParametersHandler

object Main extends App {
  val parameters = ParametersHandler.parameters
  print("Считанные параметры: ")
  println(parameters.mkString(", "))

  val sourceConnectionDescriptions = new SourceConnectionDescriptions

  val arr = sourceConnectionDescriptions.findname(parameters(BUSINESS_SYSTEM_NAME))

  val jdbcAuth = new AuthSource(
    arr(0),
    arr(1),
    arr(2),
    parameters(BUSINESS_SYSTEM_USER_NAME),
    parameters(BUSINESS_SYSTEM_USER_PASSWORD))

  val connection = jdbcAuth.getConnection()

  try {

    var sourceHandler = new SourceHandler(connection, parameters(TABLE_NAME))

    sourceHandler.checkSource()

    var resultSet = sourceHandler.getResultSet()

    while (resultSet.next()) {
      val id = resultSet.getLong("id")
      val name = resultSet.getString("name")
      val dt = resultSet.getTimestamp("dt")
      println("id, name, dt: " + id + ", " + name + ", " + dt)
    }

    if (parameters.get(INCREMENT_FIELD).nonEmpty) {
      val sourceHandler = new SourceHandler(connection, parameters(TABLE_NAME), parameters(INCREMENT_FIELD))

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
