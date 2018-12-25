package com.spark.project

import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}

object Regression_Frank extends App with SparkContextClass {

  val testingLines, trainingLines = spark.sparkContext.textFile(total_general_path + "/SparkScala/regression.txt")

  val trainingData = trainingLines.map(LabeledPoint.parse).cache()
  val testData = testingLines.map(LabeledPoint.parse).cache()

  val MyInstOfalgorithm = new LinearRegressionWithSGD()

  MyInstOfalgorithm.optimizer
    .setNumIterations(100)
    .setStepSize(1.0)
    .setUpdater(new SquaredL2Updater())
    .setRegParam(0.01)

val MyInstOfModel = MyInstOfalgorithm.run(trainingData)

val predictions = MyInstOfModel.predict(testData.map(_.features))

val predictionAndLabel = predictions.zip(testData.map(_.label))

for (prediction <- predictionAndLabel)  {
  println(prediction)
}


}
