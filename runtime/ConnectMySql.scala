import java.sql.{DriverManager, ResultSet}
import cn.cnic.bigdatalab.compute.DataChainCompute

/**
  * Created by Flora on 2016/7/20.
  */
class ConnectMySql extends DataChainCompute{
  val user="root"
  val password = "root"
  val host="10.0.71.1"
  val database="spark"
  val conn_str = "jdbc:mysql://"+host +":3306/"+database+"?user="+user+"&password=" + password
  Class.forName("com.mysql.jdbc.Driver").newInstance();
  val conn = DriverManager.getConnection(conn_str)
  println(conn_str)

  override def process(schema: String, line: String): Unit ={
    println("hello")
    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)

      // Execute Query
      val rs = statement.executeQuery("SHOW TABLES")

      // Iterate Over ResultSet
      while (rs.next) {
        println(rs.getRow())
      }

      var prep = conn.prepareStatement("INSERT INTO test (id, name, age) VALUES (?, ?, ?) ")

      var i:Int = 1
      val columns = line.split(",")
      for( col <- schema.split(",")){
        prep = ConvertColumn.parseDataType(prep, i, col.split(":")(1), columns(i-1))
        i = i + 1
      }
      prep.executeUpdate
    }
    catch {
      case ex : Exception => println(ex.getMessage)
    }
    finally {
      conn.close
    }
  }

}
