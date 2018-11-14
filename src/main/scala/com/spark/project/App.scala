package com.spark.project





import scala.math.max




/**
 * Hello world!
  *
 *maven->install; maven-> compile;
  * java -classpath "target/sparksca-1.0-SNAPSHOT-jar-with-dependencies.jar" com.spark.project.myObj
  *mvn clean cole
  *
  * with dependency!!! mvn clean compile assembly:single
  * Указав предварительно мэйн класс
  * при проблемах с maven - > rm -R  ~.m2/ -> reimport -> mvn clean install -X
  * <plugin>
  * <artifactId>maven-assembly-plugin</artifactId>
  *......
  * <manifest>
  * <mainClass>com.spark.project.ALM_model_spark2</mainClass>
  * </manifest>
  * </archive>
  * <descriptorRefs>
  * <descriptorRef>jar-with-dependencies</descriptorRef>
  * </descriptorRefs>
  * </configuration>
  * </plugin>
**/




object WordCount extends App with SparkContextClass {

    //WordCound RDD сортировка
  val text = spark.sparkContext.textFile("/home/boris/Рабочий стол/SparkScalaCource/u.data")

  val counts = text.map(x => x.toString().split("\t")(2))

  val results = counts.countByValue()

  val sortedResults = results.toSeq.sortBy(_._1)

  sortedResults.foreach(println)


}



object AverageNumberOfFriends extends App with SparkContextClass {

  //Считаем среднее количество по ключу
  //RDD

  def parseLine(line:String) = {

    val fields = line.split(",")
    val age = fields(2).toInt
    val name = fields(1)
    val numFriends = fields(3).toInt

    (name, numFriends)

  }

  val lines = spark.sparkContext.textFile("/home/boris/Рабочий стол/SparkScalaCource/SparkScala/fakefriends.csv")

  val rdd = lines.map(parseLine)

  val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
  rdd.mapValues(x=>(x,1))

  val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)

  val results = averagesByAge.collect()

  results.sorted.foreach(println)

}






object BaseRecepiesSpark extends App with SparkContextClass {

  //Выводим разные статистики по датасету
  // Конвертим csv в sql.DataSet
  // DataSet

 val linesData = spark.read.csv("/home/boris/Рабочий стол/SparkScalaCource/SparkScala/1800.csv")

  val linesData2 = spark
    .read
    .option("header", "false")
    .option("nullValue","?") //??????
    .option("inferSchema","true") // Автоматически подбираем тип данных
    .csv("/home/boris/Рабочий стол/SparkScalaCource/SparkScala/1800.csv")

val linesData2AfterFilling = linesData2.na.fill("eee", Seq("_c5")) //Заполнение нулевых значений значениями

linesData2.show()
linesData2.printSchema() // Показать данные по полям

linesData2AfterFilling.describe().show(100)//вывести средние статистики по полям

  //Эквиваленты на SQL и DataFrame для данных из csv файла-------------------------
linesData2AfterFilling.cache()
linesData2AfterFilling.groupBy("_c2").count().orderBy()//.show()

linesData2AfterFilling.createOrReplaceTempView("Data1800")   //Переходим к работе от csv к sql
spark.sql("""select _c2, COUNT(*) cnt from Data1800 group by _c2 order by cnt desc""")//.show(20)


}


object AmountSpentByCustomer extends App with SparkContextClass {

  //Считаем суммы по ключу на уровне RDD
  //После чего делаем сортировку

  def extractorFromFile(line:String) = {

    val fields = line.split(",")
    (fields(0), fields(2).toFloat)

  }


  val input = spark.sparkContext.textFile("/home/boris/Рабочий стол/SparkScalaCource/SparkScala/customer-orders.csv")

  val MRinput = input.map(extractorFromFile).reduceByKey((x,y) => x + y)

  val AgainMapped = MRinput.map(x=> (x._1, x._2))

  val SortMRInput = AgainMapped
    .sortBy(_._2)
    .collect()// Забавно, без вызова коллекта сортировка реально не выполняется

  SortMRInput.foreach(println)

}



object TheMostPercepetationDayForDistrict extends App with SparkContextClass {

  //Определить наиболее осадочный день по станциям наблюдения
  //GM000010962,18001021,PRCP,0,,,E,
  //RDD
  //Фильтруем по RDD
  //Преобразуем данные по RDD
  //Находим максимальное значение по всем полям или по определенным в разрезе RDD


  def ParseFunc(line:String) = {

    val fields = line.split(",")

    val stationId = fields(0)
    val dateOFMeasure = fields(1)
    val typeOFMonitoring = fields(2)
    val theQuanitityOfPercep = fields(3).toInt

    (stationId, dateOFMeasure, typeOFMonitoring, theQuanitityOfPercep)
  }

  val linesData = spark.sparkContext.textFile("/home/boris/Рабочий стол/SparkScalaCource/SparkScala/1800.csv")

  val parsedLines = linesData.map(ParseFunc)

  val percepetationType = parsedLines.filter(x => x._3 == "PRCP")


  //1) ByDistrictAndDate
  val  prepDataWithDistrictName = percepetationType.map(x => (x._1 + "_" + x._2, x._4.toFloat))

  val MaxPercepetationByDateInDistrict = prepDataWithDistrictName.reduceByKey((x,y) => max (x,y))

  //2) ByDate

  val  prepDataWithOutDistrictName = percepetationType.map(x => (x._2, x._4.toInt))

    // Max. Находим максимальное значение для ключа по значению
    val maxKey = prepDataWithOutDistrictName.max()(new Ordering[Tuple2[String, Int]]() {
      override def compare(x: (String, Int), y: (String, Int)): Int =
        Ordering[Int].compare(x._2, y._2)
    })

  val results = maxKey

  print(results)


}


object FlatMapProffWordCountWithFilterByStopWords extends App with SparkContextClass {

  //WordCount с фильтрацией стоп-слов
  // RDD
  //  Фильтрация через RDD
  //

  def parseStopWords(line:String) ={

    val fields = line.split(" ")

    val words = fields(0)
    words

  }

  //Convert .txt->RDD->Iterator->List
  val inputDataForStopWords = spark.sparkContext.textFile("/home/boris/Рабочий стол/SparkScalaCource/SparkScala/StopWords.txt")

  val parsedStopList = inputDataForStopWords.map(parseStopWords)

 val listss = parsedStopList.toLocalIterator.toList

  //The main function

  val input = spark.sparkContext.textFile("/home/boris/Рабочий стол/SparkScalaCource/SparkScala/book.txt")

  val words = input.flatMap(x => x.split("\\W+")).map(x => x.toLowerCase())

  val filteredInputs = words.filter(x => !(listss.contains(x)))

  //Простой но не оптимальный путь расчета частоты слов
//  val wordCounts = lowercaseWords.countByValue()
//
//  wordCounts.foreach(println)

  //Оптимальный путь расчета частоты слов без перекосов

  val wordCounts = filteredInputs.map(x => (x,1)).reduceByKey((x,y)=> x + y)

  val wordCountSorted = wordCounts.map(x=>(x._2,x._1)).sortByKey(ascending = false)


  for (result <- wordCountSorted) {
    val count = result._1
    val word = result._2
    println(s"$word:$count")
  }


}




object FilterWeatherDataMin extends App with SparkContextClass {

  //RDD
  //Фильтрация по RDD
  //Предварительная сортировка в цикле

  def parseLine(line:String)= {

    val fields = line.split(",")

    val stationId = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f //конвертируем в фаренгейт

    (stationId, entryType, temperature)

  }

  val lines = spark.sparkContext.textFile("/home/boris/Рабочий стол/SparkScalaCource/SparkScala/1800.csv")

  val parsedLines = lines.map(parseLine)

  val minTemps = parsedLines.filter(x => x._2 == "TMAX")

  val stationTemps = minTemps.map(x=> (x._1, x._3.toFloat))

  val minTempsByStation = stationTemps.reduceByKey( (x, y) => max(x,y))

  val results = minTempsByStation.collect()

  for (result <- results.sorted) {

    val station = result._1
    val temp = result._2
    val formattedTemp = f"$temp%.2f F"

    println(s"$station MAX temperature: $formattedTemp")


  }




}



object Fib extends App {

  /*
  function fibonacchi(n){
  if ( n == 0 ) return 0;

  if (n == 1) return 1;
  return fibonacchi(n-2) + fibonacchi(n-1);
}
   */

  var n:Int = 0

  def fibon(n:Int): Int = {

    if (n == 0) return 0;

    if (n == 1) return 1;

    return fibon(n-2) + fibon(n-1)
  }


  for (x<-0 to 9) print(fibon(x))


}



