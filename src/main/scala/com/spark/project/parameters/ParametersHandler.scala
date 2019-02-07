package com.spark.project.parameters

import java.io.{File, FileInputStream}

import com.spark.project.SourceConnectionDescriptions
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConversions.mapAsScalaMap

object ParametersHandler {

  val parameters: Map[String, String] = getParameters

  private def getParameters: Map[String, String] = {
    val yaml = new Yaml()
    val file = new File("params.yml")
    val inputStream = new FileInputStream(file)
    val scd = new SourceConnectionDescriptions
    try {
      val params: java.util.Map[String, String] = yaml.load(inputStream)
      var map = mapAsScalaMap(params)

      val connectionParams = scd.findname(map(Parameters.SYSTEM))
      map(Parameters.HOST) = connectionParams(0)
      map(Parameters.PORT) = connectionParams(1)
      map(Parameters.SID) = connectionParams(2)
      map(Parameters.DB_TYPE) = connectionParams(3)

      map = map.filter{case (_, value) => value != null && value.trim.nonEmpty}
        .map{case (key, value) => (key, value.trim)}

      if (!Parameters.necessaryParameters.subsetOf(map.keySet)) {
        throw new Exception("Конфигурационный файл не содержит всех необходимых параметров.")
      }
      map.toMap
    } catch {
      case e: Throwable =>
        println("Ошибка во время считывания файла параметров.")
        throw e
    } finally {
      inputStream.close()
    }
  }
}
