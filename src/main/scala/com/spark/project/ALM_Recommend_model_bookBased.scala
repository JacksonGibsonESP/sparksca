package com.spark.project

import org.apache.spark.sql.DataFrame



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



  val general_path = "/home/boris/Рабочий стол/SparkScalaCource/SparkScala/Recommendation/BooksRecommendation/"



  println("---------------------------------------------------------------------books_ratings------------------------------------------------------------------------")
  val books_ratings = new ParseObj(general_path+"BX-Book-Ratings.csv")
  books_ratings.show_stats_for_date()

  val books_rating_data = books_ratings.parsing_data_src()
  books_rating_data.show(20, false)

  println("---------------------------------------------------------------------books------------------------------------------------------------------------")
  val books = new ParseObj(general_path+"BX-Books.csv")
  books.show_stats_for_date()

  val books_data = books.parsing_data_src()
  books_data.show(20, false)

  println("----------------------------------------------------------------------user------------------------------------------------------------------------")
  val user = new ParseObj(general_path+"BX-Users.csv")
  user.show_stats_for_date()

  val user_data = user.parsing_data_src()
  user_data.show(20, false)



//  def preparation(input_src_data:String): Int, String, Int = {
//
//
//      val fields = input_src_data.split(";")
//      val User_ID = fields(0).toInt
//      val ISBN = fields(1)
//      val Books_rating = fields(2).toInt
//
//      (User_ID, ISBN, Books_rating)
//
//  }
//
//
//  val rdd = book_ratings.map(preparation)







}
