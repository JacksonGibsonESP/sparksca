package com.spark.project

import com.spark.project.auth.AuthIdentifier
import com.spark.project.parameters.{ArgumentsHandler, Parameters}

object Main extends App {
  val argHandler = new ArgumentsHandler("prod")
  val parameters = argHandler.getParameters
  print("Считанные параметры: ")
  println(parameters.mkString(", "))

  println(parameters.get(Parameters.AGE))

  val jdbcAuth = new AuthIdentifier



}
