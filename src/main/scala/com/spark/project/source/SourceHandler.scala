package com.spark.project.source

import java.sql.Connection

import com.spark.project.parameters.Parameters._
import com.spark.project.parameters.ParametersHandler

object SourceHandler {
  def getSource(connection: Connection): Source = {
    val parameters = ParametersHandler.parameters
    val dbType = parameters(DB_TYPE)

    if (dbType == ORACLE) {
      if (parameters(LOAD_TYPE) == FULL_LOAD_TYPE) {
        new OracleSource(connection, parameters(OBJECT_NAME), parameters(SCHEMA))
      } else {
        //В LOAD_TYPE находится имя колонки для инкремента
        new OracleSource(connection, parameters(OBJECT_NAME), parameters(SCHEMA), parameters(LOAD_TYPE))
      }
    } else {
      if (parameters(LOAD_TYPE) == FULL_LOAD_TYPE) {
        new MsSqlSource(connection, parameters(OBJECT_NAME))
      } else {
        //В LOAD_TYPE находится имя колонки для инкремента
        new MsSqlSource(connection, parameters(OBJECT_NAME), parameters(LOAD_TYPE))
      }
    }
  }
}
