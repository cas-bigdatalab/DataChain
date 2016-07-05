package cn.cnic.bigdatalab.entity

import cn.cnic.bigdatalab.transformer.tools
import cn.cnic.bigdatalab.utils.PropertyUtil

import scala.collection.mutable.ArrayBuffer
import scala.tools.nsc.Driver
import scala.util.parsing.json.JSON

/**
  * Created by Flora on 2016/6/23.
  */
class Schema {

  var driver:String = _
  var db:String = _
  var table:String = _
  var columns:Map[String, String] = _


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

  def getColumns(): Map[String, String] ={
    columns
  }

  def setColumns(columns: Map[String, String]): Schema ={
    this.columns = columns
    this
  }

  def columnsToString(): String ={
    val columnsBuffer:StringBuffer = new StringBuffer()

    var i = 1
    for((key,value) <- columns){
      columnsBuffer.append(key + " " + value)
      if(i < columns.size){
        columnsBuffer.append(", ")
      }
      i = i + 1
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
    assert(!map.get("columns").get.asInstanceOf[Map[String,String]].isEmpty)
    val  columns = map.get("columns").get.asInstanceOf[Map[String,String]]

    schema.setDriver(driver).setTable(table).setColumns(columns)
  }

}