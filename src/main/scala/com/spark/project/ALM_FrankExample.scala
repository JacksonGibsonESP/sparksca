package com.spark.project

import java.nio.charset.CodingErrorAction

import org.apache.spark.HashPartitioner

import scala.io.{Codec, Source}
import scala.math._

object MovieSimilarities extends SparkContextClass with App{

  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {

    //?????

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()

    val lines = Source.fromFile(total_general_path + "/u.item").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    return movieNames
  }


  // Создаем новые типы данных
  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))


  def makePairs(userRatings:UserRatingPair) = {
    ///??????

    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val rating1 = movieRating1._2
    val movie2 = movieRating2._1
    val rating2 = movieRating2._2

    ((movie1, movie2), (rating1, rating2))
  }

  def filterDuplicates(userRatings:UserRatingPair):Boolean = {
    // Убираем лишние пары

    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val movie2 = movieRating2._1

    return movie1 < movie2
  }

  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]

  def computeCosineSimilarity(ratingPairs:RatingPairs): (Double, Int) = {
    // ????


    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator:Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)

    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }

    return (score, numPairs)
  }

  /** Основная часть */


    println("\nLoading movie names...")
    val nameDict = loadMovieNames()

    val data = spark.sparkContext.textFile(total_general_path + "/u.data")

    // ??????????????????????????????
    val ratings = data
      .map(l => l.split("\t"))
      .map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble)))

    // Соединяем фильмы от одного пользователя в пары
    val joinedRatings = ratings.join(ratings)

    // Фильтруем ненужные пары
    val uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

    // Используем партицирование
    // Его рекомендовано использовать перед большими операциями:
    // Join(), cogroup(), groupWith(), leftOuterJoin(), rightOuterJoin(),
    // groupByKey(), reduceByKey(), combineByKey(), lookup()
    // число партиций - минимум ровно числу исполнителей на кластере
    // 100 -базовое число для начала
   //


    val moviePairs = uniqueJoinedRatings.map(makePairs).partitionBy(new HashPartitioner(100))

    // ????????
    val moviePairRatings = moviePairs.groupByKey()

//     ????????
    val moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

    //Save the results if desired
    //val sorted = moviePairSimilarities.sortByKey()
    //sorted.saveAsTextFile("movie-sims")

    // Extract similarities for the movie we care about that are "good".

//????
      val scoreThreshold = 0.97
      val coOccurenceThreshold = 50.0

      val movieID:Int = 50

      // Filter for movies with this sim that are "good" as defined by
      // our quality thresholds above

      val filteredResults = moviePairSimilarities.filter( x =>
      {
        val pair = x._1
        val sim = x._2
        (pair._1 == movieID || pair._2 == movieID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
      }
      )

      // Sort by quality score.
      val results = filteredResults.map( x => (x._2, x._1)).sortByKey(false).take(10)

      println("\nTop 10 similar movies for " + nameDict(movieID))
      for (result <- results) {
        val sim = result._1
        val pair = result._2
        // Display the similarity result that isn't the movie we're looking at
        var similarMovieID = pair._1
        if (similarMovieID == movieID) {
          similarMovieID = pair._2
        }
        println(nameDict(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
      }
    }


