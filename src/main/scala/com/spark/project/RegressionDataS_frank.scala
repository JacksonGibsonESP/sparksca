package com.spark.project

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg.Vectors

object RegressionDataS_frank extends App with SparkContextClass {


val inputLines = spark.sparkContext.textFile(total_general_path + "/SparkScala/regression.txt")

 val data = inputLines
   .map(_.split(","))
   .map(x => (x(0).toDouble, Vectors.dense(x(1).toDouble))) // y/x


  import spark.implicits._
  val colNames= Seq("label", "features")
  val df = data.toDF(colNames:_*)



  // Делаем разбивку на обучающую и испытательную
  val trainTest = df.randomSplit(Array(0.5, 0.5))
  val trainingDF = trainTest(0)
  val testDF = trainTest(1)


  //

  // Настраиваем модель
  val lir = new LinearRegression()
    .setRegParam(0.3)
    .setElasticNetParam(0.8)
    .setMaxIter(100)
    .setTol(1E-6)


  // Обучаем модель
  val model = lir.fit(trainingDF)

  // Делаем предсказание и проверку испытательной выборкой
  val fullPredictions = model.transform(testDF).cache()

  val predictionAndLabel = fullPredictions
    .select("prediction", "label")
    .rdd.map(x=> (x.getDouble(0), x.getDouble(1)))


  for (prediction <- predictionAndLabel) {
    println(prediction)
  }



}
