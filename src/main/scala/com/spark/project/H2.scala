package com.spark.project

import java.sql.DriverManager

import org.apache.spark.sql.SparkSession

object H2 extends App {

  Class.forName("org.h2.Driver").newInstance
  val conn = DriverManager.getConnection("jdbc:h2:file:/tmp/h2/test", "sa", "")
  try {
    var st = conn.createStatement

    st.execute("CREATE TABLE IF NOT EXISTS TEST(id INT, NAME VARCHAR(255))")

    st.execute("INSERT INTO TEST VALUES(default,'HELLO')")
    st.execute("INSERT INTO TEST(NAME) VALUES('JOHN')")
    val name1 = "Jack"
    val q = "insert into TEST values(1, ?)"
    var st1 = conn.prepareStatement(q)
    st1.setString(1, name1)
    st1.execute

    var result = st.executeQuery("SELECT * FROM TEST")
    while ( {
      result.next
    }) {
      val name = result.getString("NAME")
      System.out.println(result.getString("ID") + " " + name)
    }

    val spark = SparkSession
      .builder()
      //    .master("local[1]")
      .appName("Spark_JDBC_connection")
      .getOrCreate()
    spark.stop()
  } catch {
    case e: Throwable =>
      println("Ошибка во время установки соединения с базой данных.")
      throw e
  } finally {
    conn.close()
  }
}
