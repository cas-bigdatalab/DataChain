package cn.cnic.bigdatalab.entity

import cn.cnic.bigdatalab.utils.PropertyUtil

import scala.tools.nsc.Driver

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