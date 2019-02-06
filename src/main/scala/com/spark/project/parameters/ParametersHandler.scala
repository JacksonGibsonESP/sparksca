package com.spark.project.parameters

import java.io.{File, FileInputStream}

import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConversions.mapAsScalaMap

object ParametersHandler {

  val parameters: Map[String, String] = getParameters

  private def getParameters: Map[String, String] = {
    val yaml = new Yaml()
    val file = new File("params.yml")
    val inputStream = new FileInputStream(file)
    try {
      val params: java.util.Map[String, String] = yaml.load(inputStream)
      val map = mapAsScalaMap(params).map{case (key, value) => (key, value.trim)}
      if (!Parameters.necessaryParameters.subsetOf(map.keySet)) {
        throw new Exception("Конфигурационный файл не содержит всех необходимых параметров.")
      }
      params.toMap
    } catch {
      case e: Throwable =>
        println("Ошибка во время считывания файла параметров.")
        throw e
    } finally {
      inputStream.close()
    }
  }
}
