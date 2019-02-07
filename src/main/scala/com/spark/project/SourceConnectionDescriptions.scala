package com.spark.project

import scala.collection.immutable.Map

class SourceConnectionDescriptions {

  val a = Map(
    "kih" -> "localhost:49161:xe:dbtypeoracle",
    "khd" -> "localhost:1433::dbtypemssql",
    "another" -> "anname:anport:ansid:dbtypeterad")

  def findname(bname: String) = {

    var b = a(bname)

    b.split(":").toArray

  }
}
