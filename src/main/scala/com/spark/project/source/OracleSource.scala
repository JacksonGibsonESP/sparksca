package com.spark.project.source

import java.sql.{Connection, ResultSet}

class OracleSource(connection: Connection, tableName: String, schema: String) extends Source {

  private var incrField: String = ""

  def this(connection: Connection, tableName: String, schema: String, incrField: String) {
    this(connection, tableName, schema)
    this.incrField = incrField
  }

  def checkSource(): Unit = {
    if (incrField.isEmpty) {
      val query = "SELECT COUNT(*) FROM " + schema + "." + tableName
      println("Исполнение запроса: " + query)

      val statement = connection.createStatement()
      val resultSet: ResultSet = statement.executeQuery(query)

      resultSet.next()
      val count = resultSet.getLong(1)
      println("Результат: " + count)

      if (count == 0) {
        throw new Exception("В таблице отсутствуют данные, соответствующие исполняемому запросу")
      }
    } else {
      val query = "SELECT COUNT(*) FROM " + schema + "." + tableName + " WHERE " + incrField +
        " = (SELECT MAX(" + incrField + ") FROM " + schema + "." + tableName + ")"
      println("Исполнение запроса: " + query)

      val statement = connection.createStatement()
      val resultSet: ResultSet = statement.executeQuery(query)

      resultSet.next()
      val count = resultSet.getLong(1)
      println("Результат: " + count)

      if (count == 0) {
        throw new Exception("В таблице отсутствуют данные, соответствующие исполняемому запросу")
      }
    }
  }

//  def getResultSet(): ResultSet = {
//    if (incrField.isEmpty) {
//      val query = "SELECT * FROM " + schema + "." + tableName
//      println("Исполнение запроса: " + query)
//
//      val statement = connection.createStatement()
//      statement.executeQuery(query)
//    } else {
//      val query = "SELECT * FROM " + schema + "." + tableName + " WHERE " + incrField +
//        " = (SELECT MAX(" + incrField + ") FROM " + schema + "." + tableName + ")"
//      println("Исполнение запроса: " + query)
//
//      val statement = connection.createStatement()
//      val resultSet: ResultSet = statement.executeQuery(query)
//      statement.executeQuery(query)
//    }
//  }
}
