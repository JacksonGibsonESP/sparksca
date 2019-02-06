package com.spark.project.parameters


object Parameters {
  //JDBC connection parameters:
  val JDBC_HOSTNAME = "jdbcHostName"
  val JDBC_PORT = "jdbcPort"
  val JDBC_SID = "jdbcSid"
  val JDBC_USERNAME = "jdbcUserName"
  val JDBC_USERPASSWORD = "jdbcUserPassword"

  //Data source information:
  val TABLENAME = "tableName"
  val INCRFIELD = "incrField"

  val necessaryParameters = Set(
    JDBC_HOSTNAME,
    JDBC_PORT,
    JDBC_SID,
    JDBC_USERNAME,
    JDBC_USERPASSWORD,
    TABLENAME)
}
