package com.spark.project

import org.apache.spark.sql.DataFrame

import scala.collection.mutable

trait parseObjTrait extends ParseObj


class ParseObj(val SrcpathString : String) extends SparkContextClass {

  val external_path = SrcpathString


  def parsing_data_src():(DataFrame) = {
    val inner_src_path = external_path
    val temp_frame = spark
      .read
      .option("delimiter", ";")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inner_src_path)

    temp_frame
  }


  def show_stats_for_date (): Unit = {

    val parsed_data_frame = parsing_data_src()

    (parsed_data_frame.printSchema(),  parsed_data_frame.describe().show(200, false))

  }


}


object ALM_Recommend_model_bookBased extends App with SparkContextClass {



val general_path = total_general_path + "/SparkScala/Recommendation/BooksRecommendation/"

val list_of_names = Seq("BX-Book-Ratings.csv", "BX-Books.csv", "BX-Users.csv")


// Парсим по входящему списку файликов, с целью получения Датафреймов и статистик по ним
val dfs = mutable.ListBuffer[DataFrame]()
for (file <- list_of_names) yield {

  val obj = new ParseObj(general_path + file)
  obj.show_stats_for_date()

  println(s"-------------------------------------------------------------------------$file-----------------------------------------------------------------------")
  val someDF = obj.parsing_data_src()
  someDF.show(20, false)

  dfs += someDF
}

val firstDF = dfs(0)
val secondDF = dfs(1)
val thirdDF = dfs(2)



}
