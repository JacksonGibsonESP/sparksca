package com.spark.project.source

import scala.collection.immutable.Map

class SourceConnectionDescriptions {

  val a = Map(
    "kih" -> "kiname:kiport:kisid:dbtypeoracle",
    "khd" -> "khname:khport:khsid:dbtypemssql",
    "another" -> "anname:anport:ansid:dbtypeterad")

  def findname(bname: String): Array[String] = {
    try {
      val b = a(bname)
      b.split(":")
    } catch {
      case e : Throwable =>
        throw new Exception("Система " + bname + " отсутствует в справочнике.", e)
    }
  }
}
