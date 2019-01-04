package com.spark.project

import java.net.{MalformedURLException, URL}

import org.jgrapht.alg.shortestpath.DijkstraShortestPath
import org.jgrapht.graph.{DefaultDirectedGraph, DefaultEdge, SimpleGraph}
import scala.collection.JavaConverters._

object HelloJGraphT {
  /**
    * The starting point for the demo.
    *
    * @param args ignored.
    */
  def main(args: Array[String]): Unit = {
    val stringGraph = createStringGraph
    // note undirected edges are printed as: {<v1>,<v2>}
    println(stringGraph.toString)
    // create a graph based on URL objects
    val hrefGraph = createHrefGraph
    // note directed edges are printed as: (<v1>,<v2>)
    System.out.println(hrefGraph.toString)
    val dg = createDiGraph
    val dp = new DijkstraShortestPath[String, DefaultEdge](dg)

    dp.getPath("v1", "v4")
      .getEdgeList
      .stream
      .iterator().asScala.foreach(println)
  }

  /**
    * Creates a toy directed graph based on URL objects that represents link structure.
    *
    * @return a graph based on URL objects.
    */
  private def createHrefGraph = {
    val g = new DefaultDirectedGraph[URL, DefaultEdge](classOf[DefaultEdge])
    try {
      val amazon = new URL("http://www.amazon.com")
      val yahoo = new URL("http://www.yahoo.com")
      val ebay = new URL("http://www.ebay.com")
      // add the vertices
      g.addVertex(amazon)
      g.addVertex(yahoo)
      g.addVertex(ebay)
      // add edges to create linking structure
      g.addEdge(yahoo, amazon)
      g.addEdge(yahoo, ebay)
    } catch {
      case e: MalformedURLException =>
        e.printStackTrace()
    }
    g
  }

  /**
    * Create a toy graph based on String objects.
    *
    * @return a graph based on String objects.
    */
  private def createStringGraph = {
    val g = new SimpleGraph[String, DefaultEdge](classOf[DefaultEdge])
    val v1 = "v1"
    val v2 = "v2"
    val v3 = "v3"
    val v4 = "v4"
    g.addVertex(v1)
    g.addVertex(v2)
    g.addVertex(v3)
    g.addVertex(v4)
    // add edges to create a circuit
    g.addEdge(v1, v2)
    g.addEdge(v2, v3)
    g.addEdge(v3, v4)
    g.addEdge(v4, v1)
    g
  }

  private def createDiGraph = {
    val g = new DefaultDirectedGraph[String, DefaultEdge](classOf[DefaultEdge])
    val v1 = "v1"
    val v2 = "v2"
    val v3 = "v3"
    val v4 = "v4"
    g.addVertex(v1)
    g.addVertex(v2)
    g.addVertex(v3)
    g.addVertex(v4)
    g.addEdge(v1, v2)
    g.addEdge(v2, v3)
    g.addEdge(v3, v4)
    g.addEdge(v4, v1)
    g.addEdge(v2, v4)
    g
  }
}

final class HelloJGraphT private() // ensure non-instantiability.
{
}