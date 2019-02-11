package com.spark.project.parameters

object Parameters {
  //Data source information:
  val LOGIN_SOURCE = "LOGIN_SOURCE"
  val PASSWORD_SOURCE = "PASSWORD_SOURCE"
  val SYSTEM = "SYSTEM"
  val SCHEMA = "SCHEMA"
  val HOST = "HOST"
  val PORT = "PORT"
  val SID = "SID"
  val DB_TYPE = "DB_TYPE"
  val OBJECT_NAME = "OBJECT_NAME"
  val LOAD_TYPE = "LOAD_TYPE"

  //Target information
  val HADOOP_FILE_PATH = "HADOOP_FILE_PATH"
  val HIVE_TABLE_NAME = "HIVE_TABLE_NAME"
  val SAVE_DATA_TYPE = "SAVE_DATA_TYPE"
  val COMPRESS_FORMAT = "COMPRESS_FORMAT"
  val TABLE_SAVE_MODE = "TABLE_SAVE_MODE"

  //Constants:
  val FULL_LOAD_TYPE = "full"
  val ORACLE = "dbtypeoracle"
  val MSSQL = "dbtypemssql"
  val TERADATA = "dbtypeterad"

  val necessaryParameters = Set(
    LOGIN_SOURCE,
    PASSWORD_SOURCE,
    SYSTEM,
    SCHEMA,
    OBJECT_NAME,
    LOAD_TYPE,
    HADOOP_FILE_PATH,
    HIVE_TABLE_NAME)
}
