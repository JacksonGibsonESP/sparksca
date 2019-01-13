package com.spark.project

import java.net.{MalformedURLException, URL}

import org.jgrapht.alg.shortestpath.DijkstraShortestPath
import org.jgrapht.graph.{DefaultDirectedGraph, DefaultEdge, DefaultUndirectedGraph}

import scala.collection.JavaConverters._

object HelloJGraphT extends App {
  /**
    * The starting point for the demo.
    *
    **/


  //scala magic
  //using abstract class without init

  //val stringGraph = createStringGraph
  // note undirected edges are printed as: {<v1>,<v2>}
  //println(stringGraph.toString)
  // create a graph based on URL objects
 // val hrefGraph = createHrefGraph
  // note directed edges are printed as: (<v1>,<v2>)
  //System.out.println(hrefGraph.toString)
  val dg = createDiGraph
  val dp = new DijkstraShortestPath[String, DefaultEdge](dg)


  // scala magic
  //https://stackoverflow.com/questions/44662647/foreach-in-scala-shows-expected-consumer-path-actual-path-boolean
 println("")


  dp.getPath("TabCol1", "TabCol4")
    .getEdgeList
    .stream
    .iterator().asScala.foreach(println)

  println("")

  dp.getPath("TabCol2", "TabCol5")
    .getEdgeList
    .stream
    .iterator().asScala.foreach(println)

  println("")

  print(dp.getPaths("TabCol4")
    .getGraph)



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
//  private def createStringGraph = {
//    val g = new SimpleGraph[String, DefaultEdge](classOf[DefaultEdge])
//    val v11 = "v11"
//    val v22 = "v22"
//    val v33 = "v33"
//    val v44 = "v44"
//    g.addVertex(v11)
//    g.addVertex(v22)
//    g.addVertex(v33)
//    g.addVertex(v44)
//    // add edges to create a circuit
//    g.addEdge(v11, v22)
//    g.addEdge(v22, v33)
//    g.addEdge(v33, v44)
//    g.addEdge(v44, v11)
//    g
//  }

  private def createDiGraph = {
    val g = new DefaultUndirectedGraph[String, DefaultEdge](classOf[DefaultEdge])

//    val TabCol1 = "TabCol1"
//    val TabCol2 = "TabCol2"
//    val TabCol3 = "TabCol3"
//    val TabCol4 = "TabCol4"
//    val TabCol12 = "TabCol12"
//    val TabCol23 = "TabCol23"
//    val TabCol5 = "TabCol5"
//    val TabCol6 = "TabCol6"
//    val TabCol56 = "TabCol56"
//    val TabCol53 = "TabCol53"
//
//    g.addVertex(TabCol1)
//    g.addVertex(TabCol2)
//    g.addVertex(TabCol3)
//    g.addVertex(TabCol12)
//    g.addVertex(TabCol23)
//    g.addVertex(TabCol5)
//    g.addVertex(TabCol6)
//    g.addVertex(TabCol53)
//    g.addVertex(TabCol56)
//
//
//
//    g.addEdge(TabCol1, TabCol12)
//    g.addEdge(TabCol12, TabCol2)
//    g.addEdge(TabCol3, TabCol23)
//    g.addEdge(TabCol2, TabCol23)
//    g.addEdge(TabCol5, TabCol53)
//    g.addEdge(TabCol3, TabCol53)
//    g.addEdge(TabCol5, TabCol56)
//    g.addEdge(TabCol6, TabCol56)

    val TabCol1 = "TabCol1"
    val TabCol2 = "TabCol2"
    val TabCol3 = "TabCol3"
    val TabCol4 = "TabCol4"
    val TabCol5 = "TabCol5"

    g.addVertex(TabCol1)
    g.addVertex(TabCol2)
    g.addVertex(TabCol3)
    g.addVertex(TabCol4)
    g.addVertex(TabCol5)
    // add edges to create a circuit
    g.addEdge(TabCol1, TabCol2)
    g.addEdge(TabCol2,TabCol3)
    g.addEdge(TabCol3, TabCol4)
    g.addEdge(TabCol5, TabCol1)

    g


  }


}