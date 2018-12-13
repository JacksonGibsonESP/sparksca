

import java.sql._


object ScalaOracleQueries extends App {


  Class.forName("oracle.jdbc.driver.OracleDriver")

  // connect to the database named "mysql" on the localhost
  val driver = "com.oracle.jdbc.Driver"
  val url = "jdbc:oracle:thin:@localhost:49161:xe"
  val username = "system"
  val password = "oracle"

  // there's probably a better way to do this
  var connection:Connection = null

  try {
    // make the connection
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)

    // create the statement, and run the select query
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery("select sys_context('USERENV','SESSION_SCHEMA') from dual")
//    while ( resultSet.next() ) {
//      val host = resultSet.getString("host")
//      val user = resultSet.getString("user")
//      println("host, user = " + host + ", " + user)
//    }
  } catch {
    case e => e.printStackTrace
  }
  connection.close()

}