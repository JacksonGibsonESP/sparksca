package com.spark.project.parameters

import java.io.{File, FileInputStream}

import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConversions.mapAsScalaMap

class ParametersHandler(val envName: String) {

  def getParameters: Map[String, String] = {
    val yaml = new Yaml()
    val file = new File("params.yml")
    val inputStream = new FileInputStream(file)
    try {
      val envs: java.util.Map[String, java.util.Map[String, String]] = yaml.load(inputStream)
      val envParameters: java.util.Map[String, String] = envs.get(envName)
      if (envParameters == null) {
        throw new Exception("Не найдены параметры для запрошенного стенда: " + envName)
      }
      val map = mapAsScalaMap(envParameters).map{case (key, value) => (key, value.trim)}
      if (!Parameters.necessaryParameters.subsetOf(map.keySet)) {
        throw new Exception("Конфигурационный файл не содержит всех необходимых параметров.")
      }
      envParameters.toMap
    } catch {
      case e: Throwable =>
        println("Ошибка во время считывания файла параметров.")
        throw e
    } finally {
      inputStream.close()
    }
  }
}
