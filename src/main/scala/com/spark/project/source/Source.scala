package com.spark.project.source

//import java.sql.ResultSet

trait Source {
  def checkSource(): Unit
//  def getResultSet(): ResultSet
}
