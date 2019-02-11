package com.spark.project

import com.spark.project.auth.AuthHandler
import com.spark.project.parameters.ParametersHandler
import com.spark.project.source.SourceHandler

object Main extends App {
//  ParametersHandler.initParametersFromFile()
  ParametersHandler.initParametersFromArgs(args)
  val parameters = ParametersHandler.parameters
  print("Считанные параметры: ")
  println(parameters.mkString(", "))

  val connection = AuthHandler.getAuth().getConnection()

  try {
    val source = SourceHandler.getSource(connection)

    source.checkSource()

//    val resultSet = source.getResultSet()
//
//    while (resultSet.next()) {
//      val id = resultSet.getLong("id")
//      val name = resultSet.getString("name")
//      val dt = resultSet.getTimestamp("dt")
//      println("id, name, dt: " + id + ", " + name + ", " + dt)
//    }
  } catch {
    case e: Throwable =>
      println("Ошибка во время установки соединения с базой данных.")
      throw e
  } finally {
    connection.close()
  }
}
