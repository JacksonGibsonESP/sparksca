package com.spark.project


import java.io.File

import scala.io.Source

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.SparkSession


object ALM_Recommend_model_filmBased extends App with SparkContextClass {



val sc = spark.sparkContext
    // load personal ratings


  /** Load ratings from file. */
  // Функция которая парсит файлик рейтингов и приводит к формату для использование в модели

  def loadRatings(path: String): Seq[Rating] = {

    print ("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<Start Load ratings from file>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    val lines = Source.fromFile(path).getLines()
    val ratings = lines.map { line =>
      val fields = line.split("::")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }.filter(_.rating > 0.0)
    if (ratings.isEmpty) {
      sys.error("No ratings provided.")
    } else {
      ratings.toSeq
    }


  }



  /** Compute RMSE (Root Mean Squared Error). */
  // Функция которая рассчитывает значение корня квадратной ошибки
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
    print ("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<computeRmse>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
      .join(data.map(x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }




    //Загружаем персональные рейтинги в RDD
    val myRatings = loadRatings("/home/boris/Рабочий стол/SparkScalaCource/SparkScala/Recommendation/FilmsCompetition/spark-training/machine-learning/personalRatings.txt")
    val myRatingsRDD = sc.parallelize(myRatings, 1)

     //load ratings and movie titles

    val movieLensHomeDir = "/home/boris/Рабочий стол/SparkScalaCource/SparkScala/Recommendation/FilmsCompetition/spark-training"

     print ("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<ratings.dat>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

    //Функция которая парсит историчные рейтинги фильмов, вызывается в месте определения
  val ratings = sc.textFile(new File(movieLensHomeDir, "ratings.dat").toString).map { line =>
    val fields = line.split(",")
    // format: (timestamp % 10, Rating(userId, movieId, rating))
    (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
  }

print("---------------------------------------------------------------------see ratings-----------------------------------------------------------------------------")
print(ratings)




// Функция которая парсит маппинг о фильмах, вызывается в месте определения
  print ("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<movies.dat>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    val movies = sc.textFile(new File(movieLensHomeDir, "movies.dat").toString).map { line =>
      val fields = line.split(",")
      // format: (movieId, movieName)
      (fields(0).toInt, fields(1))
    }.collect().toMap


// количество строк в файлике рейтингов
    val numRatings = ratings.count()

// количесто уникальных пользователей в файлике рейтингов
    val numUsers = ratings.map(_._2.user).distinct().count()

 // количесто уникальных фильмов в файлике рейтингов
    val numMovies = ratings.map(_._2.product).distinct().count()

  //Выводим количества
    println("Got " + numRatings + " ratings from "
      + numUsers + " users on " + numMovies + " movies.")

    // split ratings into train (60%), validation (20%), and test (20%) based on the
  // делим входные данные рейтингов на тренировку, валидацию, тестирование
    // last digit of the timestamp, add myRatings to train, and cache them

    val numPartitions = 4

    val training = ratings.filter(x => x._1 < 6)
      .values
      .union(myRatingsRDD)
      .repartition(numPartitions)
      .cache()

    val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8)
      .values
      .repartition(numPartitions)
      .cache()

    val test = ratings.filter(x => x._1 >= 8).values.cache()

    val numTraining = training.count()
    val numValidation = validation.count()
    val numTest = test.count()

    println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest)

  //--------------------------------------------------------------------------------------------------------------------------------------------------

    // train models and evaluate them on the validation set



    // параметры,
    // ранг - количество латентных факторов, транцидентно связано с количеством возможных кластеров,
    // лямбда - параметр обучения, чем выше, чем точнее обучение, но слишком высокое число может вызвать переобучение
    // количество итераций - количество проходов, чем больше, тем точнее, но вызывает использование доп.ресурсов
    val ranks = List(8, 12)
    val lambdas = List(0.1, 10.0)
    val numIters = List(10, 20)



// Инициализация стартовых параметров переменных
   var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue


    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
//---------------------------------------------------------------------------------------------------------

    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {


      //Вызываем обучение модели
      val model = ALS.train(training, rank, numIter, lambda)

      // Рассчитываем значение ошибки предсказания, проводим кросс-валидацию
      val validationRmse = computeRmse(model, validation, numValidation)


      println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
        + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")


      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }

    // evaluate the best model on the test set

    //Запускаем финальное тестирование на тестовом дата сете
    val testRmse = computeRmse(bestModel.get, test, numTest)

    println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
      + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")

    // create a naive baseline and compare it with the best model
  // Создаем наивный расчет модели - данный расчет содержит только средний рейтинг модели
  // включаем в расчет датасеты для обучения и валидации

    val meanRating = training.union(validation).map(_.rating).mean

  //Рассчитываем среднее значение ошибки для получившейся ошибки
    val baselineRmse = math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating)).mean)


  // сравниваем наивный основной расчет с финальным целевым расчетом, насколько целевой расчет получился лучше
    val improvement = (baselineRmse - testRmse) / baselineRmse * 100
    println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")

    // make personalized recommendations

  // Делаем персональную рекомендацию

  // Конвертируем набор выбранных фильмов в множество
    val myRatedMovieIds = myRatings.map(_.product).toSet

//Убираем из расчета выбранные пользователем фильмы
    val candidates = sc.parallelize(movies.keys.filter(!myRatedMovieIds.contains(_)).toSeq)


  //Применяем ранее рассчитанную модель для формирования рекомендации
    val recommendations = bestModel.get
      .predict(candidates.map((0, _)))
      .collect()
      .sortBy(- _.rating)
      .take(50)

    var i = 1
    println("Movies recommended for you:")
    recommendations.foreach { r =>
      println("%2d".format(i) + ": " + movies(r.product))
      i += 1
    }

    // clean up
    sc.stop()
  }





