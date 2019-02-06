package com.spark.project

import java.sql.{Connection, ResultSet}

class SourceHandler(val connection: Connection, val tableName: String) {

  private var incrField: String = ""

  def this(connection: Connection, tableName: String, incrField: String) {
    this(connection, tableName)
    this.incrField = incrField
  }

  def checkSource(): Unit = {
    if (incrField.isEmpty) {
      val query = "SELECT COUNT(*) FROM " + tableName
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
      val statement = connection.createStatement()
      val query = "SELECT COUNT(*) FROM " + tableName + " WHERE " + incrField + " IN (SELECT MAX(" + incrField + ") FROM " + tableName + ")"
      println("Исполнение запроса: " + query)
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
  //      val query = "SELECT * FROM " + tableName
  //      println("Исполнение запроса: " + query)
  //      val statement = connection.createStatement()
  //      statement.executeQuery(query)
  //    } else {
  //      val statement = connection.createStatement()
  //      val query = "SELECT * FROM " + tableName + " WHERE " + incrField + " IN (SELECT MAX(" + incrField + ") FROM " + tableName + ")"
  //      println("Исполнение запроса: " + query)
  //      val resultSet: ResultSet = statement.executeQuery(query)
  //      statement.executeQuery(query)
  //    }
  //  }
}
