package cn.cnic.bigdatalab.entity


import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.parsing.json.JSON

/**
  * Created by Flora on 2016/6/23.
  */
class Schema {

  var driver:String = _
  var db:String = _
  var table:String = _
  var columns:ArrayBuffer[String] = _


  def getDriver(): String ={
    driver
  }

  def setDriver(driver: String): Schema ={
    this.driver = driver
    this
  }

  def getDb(): String ={
    db
  }

  def setDb(db: String): Schema ={
    this.db = db
    this
  }

  def getTable(): String ={
    table
  }

  def setTable(table: String): Schema ={
    this.table = table
    this
  }

  def getColumns(): ArrayBuffer[String] ={
    columns
  }

  def setColumns(columns: ArrayBuffer[String]): Schema ={
    this.columns = columns
    this
  }
  def columnsToString(): String ={
    /*val columnsBuffer:StringBuffer = new StringBuffer()

    var i = 1
    for((key,value) <- columns){
      columnsBuffer.append(key + " " + value)
      if(i < columns.size){
        columnsBuffer.append(", ")
      }
      i = i + 1
    }*/
    val columnsBuffer:StringBuffer= new StringBuffer()
    for(i <- 0 until columns.size){
      columnsBuffer.append(columns(i).replace(":", " "))
      if(i < columns.length - 1){
        columnsBuffer.append(", ")
      }
    }
    columnsBuffer.toString

  }
  def columnsFieldToString(): String ={
    /*val columnsBuffer:StringBuffer = new StringBuffer()

    var i = 1
    for((key,value) <- columns){
      columnsBuffer.append(key + " " + value)
      if(i < columns.size){
        columnsBuffer.append(", ")
      }
      i = i + 1
    }*/
    val columnsBuffer:StringBuffer= new StringBuffer()
    for(i <- 0 until columns.size){
      columnsBuffer.append(columns(i).split(":")(0))
      if(i < columns.length - 1){
        columnsBuffer.append(", ")
      }
    }
    columnsBuffer.toString

  }

}

object Schema{

def parserJson(jsonStr:String, tableName:String):Schema={

  val mapping = JSON.parseFull(jsonStr).get
  val map: Map[String, Any] = mapping.asInstanceOf[Map[String, Any]].get(tableName).get.asInstanceOf[Map[String, Any]]

  parserMap(map)

}

def parserMap(map: Map[String, Any]):Schema={

  val schema = new Schema()

  //val map: Map[String, Any] = mapping.asInstanceOf[Map[String, Any]].get(tableName).get.asInstanceOf[Map[String, Any]]

  //driver
  assert(!map.get("driver").get.asInstanceOf[String].isEmpty)
  val driver = map.get("driver").get.asInstanceOf[String]

  //db
  if(!map.get("db").isEmpty){
    val db = map.get("db").get.asInstanceOf[String]
    schema.setDb(db)
  }

  //table
  assert(!map.get("table").get.asInstanceOf[String].isEmpty)
  val table = map.get("table").get.asInstanceOf[String]

  //columns
  assert(!map.get("columns").get.asInstanceOf[List[String]].isEmpty)
  val columnsArrayBuffer = ArrayBuffer[String]()
  val columnsList = map.get("columns").get.asInstanceOf[List[String]]
  columnsArrayBuffer.appendAll(columnsList)

  schema.setDriver(driver).setTable(table).setColumns(columnsArrayBuffer)
}

def parseMapList(map: Map[String, Any]) : List[Schema]={

  val schemaListBuffer : ListBuffer[Schema] = new ListBuffer[Schema]()

  //val schemaList : ArrayList[Schema] = new ArrayList[Schema]()

  map.keys.foreach(key => {
    val schemaMap = map.get(key).get.asInstanceOf[Map[String,Any]]
    val sc = parserMap(schemaMap)
    schemaListBuffer.append(sc)
  })

  schemaListBuffer.toList.asInstanceOf[List[Schema]]

}

}