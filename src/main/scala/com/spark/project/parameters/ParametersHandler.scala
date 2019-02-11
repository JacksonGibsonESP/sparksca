package com.spark.project.parameters

import java.io.{File, FileInputStream}

import com.spark.project.source.SourceConnectionDescriptions
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConversions.mapAsScalaMap

object ParametersHandler {

  private var paramsMap: Map[String, String] = _

  def parameters: Map[String, String] = {
    paramsMap
  }

  def initParametersFromFile(): Unit = {
    val yaml = new Yaml()
    val file = new File("params.yml")
    val inputStream = new FileInputStream(file)
    val scd = new SourceConnectionDescriptions
    try {
      val params: java.util.Map[String, String] = yaml.load(inputStream)
      paramsMap = checkMap(mapAsScalaMap(params).toMap)
    } catch {
      case e: Throwable =>
        println("Ошибка во время считывания файла параметров.")
        throw e
    } finally {
      inputStream.close()
    }
  }

  def initParametersFromArgs(args: Array[String]): Unit = {
    paramsMap = checkMap(args
      .map(_.split("="))
      .filter(_.length == 2)
      .map(x => (x(0), x(1)))
      .toMap[String, String])
  }

  private def checkMap(map: Map[String, String]): Map[String, String] = {
    val scd = new SourceConnectionDescriptions
    var res = map
      .filter { case (_, value) => value != null && value.trim.nonEmpty }
      .map { case (key, value) => (key, value.trim) }

    if (!Parameters.necessaryParameters.subsetOf(map.keySet)) {
      throw new Exception("Не переданы все необходимые параметры.")
    }

    val connectionParams = scd.findname(map(Parameters.SYSTEM))
    res = res + (Parameters.HOST -> connectionParams(0))
    res = res + (Parameters.PORT -> connectionParams(1))
    if (connectionParams(2).nonEmpty) {
      res = res + (Parameters.SID -> connectionParams(2))
    }
    res = res + (Parameters.DB_TYPE -> connectionParams(3))
    res
  }
}
