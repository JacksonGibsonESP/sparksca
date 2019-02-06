package com.spark.project

import scala.collection.immutable.Map

class SourceConnectionDescriptions {

  val a = Map(
    "kih" -> "localhost:49161:xe",
    "khd" -> "khname:khport:khsid",
    "another" -> "anname:anport:ansid")

  def findname(bname: String) = {

    var b = a(bname)

    b.split(":").toArray

  }
}
