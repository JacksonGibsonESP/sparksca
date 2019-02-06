package com.spark.project.parameters


object Parameters {
  //Data source information:
  val BUSINESS_SYSTEM_NAME = "businessSystemName"
  val BUSINESS_SYSTEM_USER_NAME = "businessSystemUserName"
  val BUSINESS_SYSTEM_USER_PASSWORD = "businessSystemUserPassword"
  val TABLE_NAME = "tableName"
  val INCREMENT_FIELD = "incrementField"

  val necessaryParameters = Set(
    BUSINESS_SYSTEM_NAME,
    BUSINESS_SYSTEM_USER_NAME,
    BUSINESS_SYSTEM_USER_PASSWORD,
    TABLE_NAME)
}
