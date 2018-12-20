package com.spark.project

import java.nio.charset.CodingErrorAction

import breeze.linalg.max
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.{Codec, Source}

// Посмотреть локально запущенный спарк в веб ui http://127.0.0.1:4040/stages/
// stage - отображает шаг, где спарк требует шафла для данных!!!!
// Шафла можно избежать увеличивая количество партиций
// Если задача падает по таймауту, то возможные причины
// В общем это значит что от исполнителя
// требуется слишком много, чем есть
//1. Решение - нужно больше машин
//2. Решение - каждый исполнитель требует больше памяти
//3. Решение - использовать партиционирование, чтобы разгладить использование памяти исполнителями

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
  val text = spark.sparkContext.textFile(total_general_path+"/u.data")

  val counts = text.map(x => x.toString().split("\t")(2))

  val results = counts.countByValue()

  val sortedResults = results.toSeq.sortBy(_._1)

  sortedResults.foreach(println)


}


// Поиск наиболее связанного с другим узлами узла в социальном графе
// Вывести топ-10 самых популярных
// Вывести топ-1о наименее популярных

object DegreesOfSeparation {

  // Вынести в параметры!!!!!
  val startCharacterID = 5206 //SpiderMan
  val targetCharacterID = 239 //ADAM 3,031 (who?)

  // Делаем переменную глобальной???
  var hitCounter: Option[LongAccumulator] = None

  // Определяем типы данных для узлов/структуру графа
  type BFSData = (Array[Int], Int, String)
  type BFSNode = (Int, BFSData)


  def convertToBFS(line: String): BFSNode = {

    //Парсим файл в структуру графа

    val fields = line.split("\\s+")

    val heroID = fields(0).toInt

    var connections: ArrayBuffer[Int] = ArrayBuffer()
    for (connection <- 1 to (fields.length - 1)) {
      connections += fields(connection).toInt
    }

    // Дефолтовые значения узла, 9999 - в смысле бесконечность
    var color: String = "WHITE"
    var distance: Int = 9999

    // ????
    if (heroID == startCharacterID) {
      color = "GRAY"
      distance = 0
    }

    return (heroID, (connections.toArray, distance, color))
  }

  // Параметризовать
  def createStartingRdd(sc: SparkContext): RDD[BFSNode] = {
    // Процедура старта
    val inputFile = sc.textFile("/home/boris/Рабочий стол/Themes/!Spark/SparkScalaCource/Marvel-graph.txt")
    return inputFile.map(convertToBFS)
  }


  def bfsMap(node: BFSNode): Array[BFSNode] = {

    // Достаем данные из узла
    val characterID: Int = node._1
    val data: BFSData = node._2

    val connections: Array[Int] = data._1
    val distance: Int = data._2
    var color: String = data._3

    //????
    var results: ArrayBuffer[BFSNode] = ArrayBuffer()

    // Если текущий уже серый, то помечаем дочерние как также серые
    if (color == "GRAY") {
      for (connection <- connections) {
        val newCharacterID = connection
        val newDistance = distance + 1
        val newColor = "GRAY"


        if (targetCharacterID == connection) {
          if (hitCounter.isDefined) {
            hitCounter.get.add(1)
          }
        }

        // Создаем наш новый серый узел
        val newEntry: BFSNode = (newCharacterID, (Array(), newDistance, newColor))
        results += newEntry
      }

      // окрашиваем в черный если дошли до конца
      color = "BLACK"
    }

    val thisEntry: BFSNode = (characterID, (connections, distance, color))
    results += thisEntry

    return results.toArray
  }

  /** Combine nodes for the same heroID, preserving the shortest length and darkest color. */
  def bfsReduce(data1: BFSData, data2: BFSData): BFSData = {

    // Extract data that we are combining
    val edges1: Array[Int] = data1._1
    val edges2: Array[Int] = data2._1
    val distance1: Int = data1._2
    val distance2: Int = data2._2
    val color1: String = data1._3
    val color2: String = data2._3

    // Default node values
    var distance: Int = 9999
    var color: String = "WHITE"
    var edges: ArrayBuffer[Int] = ArrayBuffer()

    // See if one is the original node with its connections.
    // If so preserve them.
    if (edges1.length > 0) {
      edges ++= edges1
    }
    if (edges2.length > 0) {
      edges ++= edges2
    }

    // Preserve minimum distance
    if (distance1 < distance) {
      distance = distance1
    }
    if (distance2 < distance) {
      distance = distance2
    }

    // Preserve darkest color
    if (color1 == "WHITE" && (color2 == "GRAY" || color2 == "BLACK")) {
      color = color2
    }
    if (color1 == "GRAY" && color2 == "BLACK") {
      color = color2
    }
    if (color2 == "WHITE" && (color1 == "GRAY" || color1 == "BLACK")) {
      color = color1
    }
    if (color2 == "GRAY" && color1 == "BLACK") {
      color = color1
    }

    return (edges.toArray, distance, color)
  }

}

  /** Our main function where the action happens */

  object GraphMainBFS extends App with SparkContextClass {

    import com.spark.project.DegreesOfSeparation._



    // Create a SparkContext using every core of the local machine
    val sc = spark.sparkContext

    // Our accumulator, used to signal when we find the target
    // character in our BFS traversal.
    hitCounter = Some(sc.longAccumulator("Hit Counter"))

    var iterationRdd = createStartingRdd(sc)

    var iteration:Int = 0
    for (iteration <- 1 to 10) {
      println("Running BFS Iteration# " + iteration)

      // Create new vertices as needed to darken or reduce distances in the
      // reduce stage. If we encounter the node we're looking for as a GRAY
      // node, increment our accumulator to signal that we're done.
      val mapped = iterationRdd.flatMap(bfsMap)

      // Note that mapped.count() action here forces the RDD to be evaluated, and
      // that's the only reason our accumulator is actually updated.
      println("Processing " + mapped.count() + " values.")

      if (hitCounter.isDefined) {
        val hitCount = hitCounter.get.value
        if (hitCount > 0) {
          println("Hit the target character! From " + hitCount +
            " different direction(s).")
          //return
        }
      }

      // Reducer combines data for each character ID, preserving the darkest
      // color and shortest path.
      iterationRdd = mapped.reduceByKey(bfsReduce)
    }









}






object SocialGraphSearching extends App with SparkContextClass {

  def countCoOccurences(line: String) = {
    var elements = line.split("\\s+")
    (elements(0).toInt, elements.length - 1)
  }

  def parseNames(line: String) : Option[(Int, String)] ={
    // Option - Используем обертку для того чтобы обеспечить работу в случая возврата пустых значений
    // Конструкция работает следующим образом - если возвращаемое значение не NULL то возвращаем Some, иначе None

    var fields = line.split('\"')

    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))
    } else {

      return None

    }
  }


  // Загружаем маппинг Герой - Его айдишник
val names = spark.sparkContext.textFile(total_general_path + "/Marvel-names.txt")
val namesRdd = names.flatMap(parseNames)


  // Загружаем социальный граф
  val lines = spark.sparkContext.textFile(total_general_path + "/Marvel-graph.txt")

  // Конвертируем в суммы вхождений для каждого айдишника
  val pairings = lines.map(countCoOccurences)

  // Объединить записи которые охватывают более одной ????????

  val totalFriendsByCharacter = pairings.reduceByKey((x,y) => x + y)


  // Переворачиваем
  val flipped = totalFriendsByCharacter.map(x=>(x._2, x._1))

  // Находим граф с максимальным количеством связей
  // Находим топ-10 самых популярных
  // Находимо топ-10 менее популярных


  val mostPopular = flipped.takeOrdered(10)(Ordering[Int].reverse.on(x=>x._1))
  val leastPopular = flipped.takeOrdered(num = 10)(Ordering[Int].on(x=>x._1))

 spark.sparkContext.parallelize({mostPopular.union(leastPopular)}.map(x=>(x._2, x._1)))
      .join(namesRdd)
      .map(x => x._2)
      .takeOrdered(20)(Ordering[Int].on(x => x._1))
      .foreach(println)


}




// Вывод самого просматриваемого фильма, с джоином через мапу

object MostPopularMovie extends App with SparkContextClass {

  def loadMovieName() : Map[Int, String] = {

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    //
    var movieNames: Map[Int, String] = Map()

    val lines = Source.fromFile(total_general_path + "/u.item").getLines()

    for (line <- lines) {

      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }

    }
      return movieNames

    }



 val lines = spark.sparkContext.textFile(total_general_path + "/u.data")

  // Делаем мапу (movieID, 1) - каждой строчке с фильмом присваиваем единицу
  val movies = lines.map(x => (x.split("\t")(1).toInt, 1))


  // Суммируем уникальные вхождения фильмов

  val moviecounts = movies.reduceByKey((x, y) => x + y)

  // Переворачиваем фильм/количество в количество/фильм

  val flipped = moviecounts.map(x => (x._2, x._1))

// Сортируем по количествам вхождений

  val sortedMovies = flipped.sortByKey()

  var nameDict = spark.sparkContext.broadcast(loadMovieName)

  // Немного магии и джоин двух таблиц
  val sortedMoviesWithNames = sortedMovies.map(x => (nameDict.value(x._2), x._1))

  sortedMovies // RDD [Int, Int]

  nameDict // Map [Int, String]

  // Собираем и выводим результат

  val results = sortedMoviesWithNames.collect()

  results.foreach(println)

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

  val lines = spark.sparkContext.textFile(total_general_path+"/SparkScala/fakefriends.csv")

  val rdd = lines.map(parseLine)

  val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
  rdd.mapValues(x=>(x,1))

  val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)

  val results = averagesByAge.collect()

  results.sorted.foreach(println)

}

object SparkOptimisationHints extends App with SparkContextClass {



  val general_path = total_general_path+"/SparkScala/Recommendation/BooksRecommendation/"

  val list_of_names = Seq("BX-Book-Ratings.csv", "BX-Books.csv", "BX-Users.csv")


  // Парсим по входящему списку файликов, с целью получения Датафреймов и статистик по ним
  val dfs = mutable.ListBuffer[DataFrame]()
  for (file <- list_of_names) yield {

    val obj = new ParseObj(general_path + file)

    println(s"-------------------------------------------------------------------------$file-----------------------------------------------------------------------")
    val someDF = obj.parsing_data_src()
    //someDF.show(20, false)

    dfs += someDF
  }

  val firstDF = dfs(0)
  val secondDF = dfs(1)
  val thirdDF = dfs(2)


  firstDF.createOrReplaceTempView("Table1")
  secondDF.createOrReplaceTempView("Table2")
  thirdDF.createOrReplaceTempView("Table3")

  spark.sql("select * from Table1").show(3,false)


}




object BaseRecepiesSpark extends App with SparkContextClass {

  //Выводим разные статистики по датасету
  // Конвертим csv в sql.DataSet
  // DataSet

 val linesData = spark.read.csv(total_general_path+"/SparkScala/1800.csv")

  val linesData2 = spark
    .read
    .option("header", "false")
    .option("nullValue","?") //??????
    .option("inferSchema","true") // Автоматически подбираем тип данных
    .csv(total_general_path+"/SparkScala/1800.csv")

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


  val input = spark.sparkContext.textFile(total_general_path+"/SparkScala/customer-orders.csv")

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

  val linesData = spark.sparkContext.textFile(total_general_path+"/SparkScala/1800.csv")

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
  val inputDataForStopWords = spark.sparkContext.textFile(total_general_path+"/SparkScala/StopWords.txt")

  val parsedStopList = inputDataForStopWords.map(parseStopWords)

 val listss = parsedStopList.toLocalIterator.toList

  //The main function

  val input = spark.sparkContext.textFile(total_general_path+"/SparkScala/book.txt")

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

  val lines = spark.sparkContext.textFile(total_general_path+"/SparkScala/1800.csv")

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



