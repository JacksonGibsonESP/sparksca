package com.spark.project.auth

import java.sql.Connection

trait Auth {
  def getConnection(): Connection
}
