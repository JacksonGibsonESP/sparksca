package com.spark.project

import jep.Jep

object Python extends App {
  var jep = new Jep()
  jep.runScript("add.py")
  val a = 2
  val b = 3
  // There are multiple ways to evaluate. Let us demonstrate them:
  jep.eval(s"c = add($a, $b)")
  val ans = jep.getValue("c").asInstanceOf[Int]
  println(ans)
//  val ans2 = jep.invoke("add", a, b).asInstanceOf[Int]
//  println(ans2)

  /*File: mnist_cnn.py
  Trains a simple convnet on the MNIST dataset.

  Gets to 99.25% test accuracy after 12 epochs
  (there is still a lot of margin for parameter tuning).
  16 seconds per epoch on a GRID K520 GPU.
    */

  jep.close()

  jep = new Jep()
  jep.runScript("mnist_cnn.py")
  val score = jep.getValue("score[0]").asInstanceOf[Double]
  val accuracy = jep.getValue("score[1]").asInstanceOf[Double]
  println(s"score is $score and accuracy is $accuracy")
}
