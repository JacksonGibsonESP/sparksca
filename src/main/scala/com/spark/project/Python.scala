package com.spark.project
import java.util

import jep.{Jep, NDArray}

object Python extends App {
  var jep = new Jep()
  jep.runScript("add.py")
  val a = 2
  val b = 3

  jep.eval(s"c = add($a, $b)")
  val ans = jep.getValue("c").asInstanceOf[Long]
  println(ans)

  val x: NDArray[AnyRef] = jep.getValue("x").asInstanceOf[NDArray[AnyRef]]

  println(x)
  println(x.getDimensions.mkString(" "))
  println(x.getData.getClass)

  val arr: Array[Long] = x.getData.asInstanceOf[Array[Long]]

  println(arr.mkString(" "))

  val obj = new SomeObject()

  jep.set("obj", obj)
  jep.eval("obj.setFloatData(x)")

  println(obj.getRawdata.mkString(" "))

  val list = jep.getValue("list").asInstanceOf[util.ArrayList[Long]]
  println(list)

  jep.close()
}
