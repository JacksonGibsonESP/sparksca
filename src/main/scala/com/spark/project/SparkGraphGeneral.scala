package com.spark.project

import org.apache.spark.graphx.{Edge, Graph}

object SparkGraphGeneral extends App with SparkContextClass {

  val vertexArray = Array(
    (1L, ("Alice", 28)),
    (2L, ("Bob", 27)),
    (3L, ("Charlie", 65)),
    (4L, ("David")),
    (5L, ("Ed", 55)),
    (6L,("Fran", 50))
  )

  val edgeArray = Array (

    Edge(2L, 1L, 7),
    Edge(2L, 4L, 2),
    Edge(3L, 2L, 4),
    Edge(3L, 6L, 3),
    Edge(4L, 1L, 1),
    Edge(5L, 2L, 2),
    Edge(5L, 3L, 8),
    Edge(5L, 6L, 3)

  )

  val vertexRDD = spark.sparkContext.parallelize(vertexArray)
  val edgeRDD = spark.sparkContext.parallelize(edgeArray)

  val graph = Graph(vertexRDD, edgeRDD)


  println("Количество вершин")
  println(graph.numVertices)

  println("------------------------------------------------------")

  println("Количество ребер")
  println(graph.numEdges)


  println("")
  //println(graph.edges(2))



}


//
//Если граф создаем из файлика
//val articles = sc.textFile("articles.txt")
//val links = sc.textFile("links.txt")
//
//val vertices = articles.map { line =>
//val fields = line.split('\t')
//(fields(0).toLong, fields(1))
//}
//
//val edges = links.map { line =>
//val fields = line.split('\t')
//Edge(fields(0).toLong, fields(1).toLong, 0)
//}
//
//val graph = Graph(vertices, edges, "").cache()
