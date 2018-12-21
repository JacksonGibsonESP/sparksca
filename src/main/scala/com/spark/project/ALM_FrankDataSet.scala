package com.spark.project

import org.apache.spark.mllib.recommendation.{ALS, Rating}

object ALM_FrankDataSet extends App with SparkContextClass {


  // Наследуем уже реализованную функцию парсинга
  val upObj = new UploadDataForSimilarities
  val nameDict = upObj.loadMovieNames()


  // Берем уже загруженный файлик с реализованного объекта - ошибка, пришлось создавать свой экземпляр!!!
  val data = spark.sparkContext.textFile(total_general_path + "/u.data")

  // Конвертируем загруженный
  // файлик в объект Rating для моделей

  val ratingsDataSrc = data
    .map(x=>x.split('\t'))
    .map(x=>Rating(x(0).toInt, x(1).toInt, x(2).toDouble)).cache()

  val rank = 8
  val numIterations = 20


  val model = ALS.train(ratingsDataSrc, rank, numIterations)

  val userID = 50

  println("\nRatings for user ID " + userID + " : ")

  val userRatings = ratingsDataSrc.filter(x=>x.user == userID)

  val myRatings = userRatings.collect()


  for (rating <- myRatings){
    println(nameDict(rating.product.toInt) + " : " + rating.rating.toString)
  }

  println("\nTop 10 recommendations:")

  val recommendations = model.recommendProducts(userID, 10)
  for (recommendation <- recommendations) {
    println(nameDict(recommendation.product.toInt) + " score " + recommendation.rating)
  }




}
