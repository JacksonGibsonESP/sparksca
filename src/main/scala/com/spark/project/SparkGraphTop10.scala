package com.spark.project

import org.apache.spark.graphx._


object SparkGraphTop10 extends App with SparkContextClass {

  def parseNames(line:String) : Option[(VertexId, String)] ={

    var fields = line.split('\"')
    if (fields.length > 1) {
      val heroID:Long = fields(0).trim().toLong
      if (heroID < 6487) {//просто условие для экономии работы алгоритма
        return Some(fields(0).trim().toLong, fields(1))
      }
    }

    return None

  }

def makeEdges(line: String) : List[Edge[Int]] = {
  import scala.collection.mutable.ListBuffer

  val edges = new ListBuffer[Edge[Int]]()

  val fields = line.split(" ")

  val origin = fields(0)

  for (x <- 1 to (fields.length - 1)) {
    edges += Edge(origin.toLong, fields(x).toLong, 0)
  }

  return edges.toList


}



  val names = spark.sparkContext.textFile(total_general_path + "/Marvel-names.txt")
  val verts = names.flatMap(parseNames)


  val lines = spark.sparkContext.textFile(total_general_path + "/Marvel-graph.txt")
  val edges = lines.flatMap(makeEdges)


  val default = "Nobody"
  val graph = Graph(verts, edges, default).cache()



  println("\nTop 10 most-connected superheroes")

  val root: VertexId = 5306

  val InitialGraph = graph
    .mapVertices((id, _) => if (id == root) 0.0 else Double.PositiveInfinity)

  val bfs = InitialGraph.pregel(Double.PositiveInfinity, 10) (


    // 1 параметр
    (id, attr, msg) => math.min(attr, msg), ///????

    triplet => {
    if (triplet.srcAttr != Double.PositiveInfinity) {
      Iterator((triplet.dstId, triplet.srcAttr + 1))
    } else {
      Iterator.empty
    }}, ///????

    (a, b) => math.min(a,b) ///????

  )
    .cache()

  bfs.vertices.join(verts)///????
    .take(100).foreach(println)

  println("\n\nDegrees from SpiderMan to ADAM 3,031")

  bfs.vertices.filter(x => x._1 == 14)///????
    .collect.foreach(println)





}
