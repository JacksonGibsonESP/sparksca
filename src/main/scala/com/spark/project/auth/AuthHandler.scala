package com.spark.project.auth

import com.spark.project.parameters.Parameters._
import com.spark.project.parameters.ParametersHandler

object AuthHandler {
  def getAuth(): Auth = {
    val parameters = ParametersHandler.parameters
    val dbType = parameters(DB_TYPE)
    if (dbType == ORACLE) {
      new OracleAuth(
        parameters(HOST),
        parameters(PORT),
        parameters(SID),
        parameters(LOGIN_SOURCE),
        parameters(PASSWORD_SOURCE))
    } else {
      new MsSqlAuth(
        parameters(HOST),
        parameters(PORT),
        parameters(SCHEMA),
        parameters(LOGIN_SOURCE),
        parameters(PASSWORD_SOURCE))
    }
  }
}
